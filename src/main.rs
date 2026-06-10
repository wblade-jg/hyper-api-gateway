use crate::services::ServiceRegistry;
use http_body_util::Full;
use hyper::{Request, Response, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use services::Service;
use std::collections::HashMap;
use std::sync::Arc;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::RwLock;
use tokio::time::Instant;

mod load_balancing;
mod round_robin;
mod services;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PORT: u16 = 8080;
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT);
    let listener = TcpListener::bind(socket).await?;

    println!("Escuchando clientes en el puerto: {PORT}");

    let service_registry = ServiceRegistry::new(8500);
    let services_map = service_registry.get_available_services();

    let service_map_for_health_checker = Arc::clone(&services_map);
    let service_map_for_proxy_server = Arc::clone(&services_map);

    tokio::spawn(async move {
        start_heath_checker(8505, service_map_for_health_checker)
            .await
            .unwrap()
    });

    tokio::spawn(async move { service_registry.start_service_registry().await.unwrap() });

    start_client_proxy_server(listener, service_map_for_proxy_server).await?;

    Ok(())
}

async fn start_heath_checker(
    port: u16,
    services_map: Arc<RwLock<HashMap<String, Service>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    let socket = UdpSocket::bind(socket_addr).await?;
    let mut buffer = [0; 1024];

    println!("Health checker escuchando en el puerto: {}", port);

    let services = Arc::clone(&services_map);

    loop {
        let (len, addr) = socket.recv_from(&mut buffer).await?;
        let datos_recibidos = &buffer[..len];

        if let Ok(mensaje) = std::str::from_utf8(datos_recibidos) {
            let mut _services = services.read().await;
            if let Some(service) = _services.get(mensaje) {
                if let Some(server) = service.get_server_from_ip(&addr.ip().to_string()) {
                    server.last_ping(Instant::now());
                } else {
                    println!("Servidor no registrado");
                }
            } else {
                println!("Servicio no encontrado");
            }
        } else {
            println!("No se puede parsear a UTF-8");
        }
    }
}

async fn start_client_proxy_server(
    listener: TcpListener,
    services_reference: Arc<RwLock<HashMap<String, Service>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (stream, _) = listener.accept().await?;
        let io_stream = TokioIo::new(stream);
        let services = Arc::clone(&services_reference);

        tokio::spawn(async move {
            if let Err(_) = http1::Builder::new()
                .serve_connection(
                    io_stream,
                    service_fn(|req| handle_request(req, Arc::clone(&services))),
                )
                .await
            {
                println!("Error sirviendo la conexion");
            }
        });
    }
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    services: Arc<RwLock<HashMap<String, Service>>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let res_body;
    let _services = services.read().await;

    if let Some(service_to_route) = _services.get(req.uri().path()) {
        let ip_server = service_to_route.get_server_instance_to_send();
        res_body = ip_server;
    } else {
        res_body = String::from("No server");
    }

    Ok(Response::new(Full::from(Bytes::from(res_body))))
}

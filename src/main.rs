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
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::services::ServiceRegistry;

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

    tokio::spawn(async move { service_registry.start_service_registry().await.unwrap() });

    start_client_proxy_server(listener, &services_map).await?;

    Ok(())
}

async fn start_client_proxy_server(
    listener: TcpListener,
    services_reference: &Arc<RwLock<HashMap<String, Service>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (stream, _) = listener.accept().await?;
        let io_stream = TokioIo::new(stream);
        let services = Arc::clone(services_reference);

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

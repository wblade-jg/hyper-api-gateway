use http_body_util::{BodyExt, Full};
use hyper::body::Buf;
use hyper::{Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use services::Service;
use std::collections::HashMap;
use std::sync::Arc;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

mod load_balancing;
mod round_robin;
mod services;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PORT: u16 = 8080;
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT);
    let listener = TcpListener::bind(socket).await?;

    println!("Escuchando clientes en el puerto: {PORT}");

    let services_map = Arc::new(RwLock::new(get_available_services()));
    let _services_map = Arc::clone(&services_map);

    tokio::spawn(async move {
        start_service_registry(Arc::clone(&_services_map))
            .await
            .unwrap()
    });

    start_client_proxy_server(listener, &services_map).await?;

    Ok(())
}

async fn start_service_registry(
    services_map: Arc<RwLock<HashMap<String, Service>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    const PORT: u16 = 8500;
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT);
    let listener = TcpListener::bind(socket).await?;

    println!("Service registry escuchando en el puerto: {PORT}");

    loop {
        let (stream, server_address) = listener.accept().await?;
        let io_stream = TokioIo::new(stream);
        let services = Arc::clone(&services_map);

        tokio::spawn(async move {
            if let Err(_) = http1::Builder::new()
                .serve_connection(
                    io_stream,
                    service_fn(|req| {
                        handle_registry(req, server_address.ip(), Arc::clone(&services))
                    }),
                )
                .await
            {
                println!("Error sirviendo conexion en el service registry");
            }
        });
    }
}

#[derive(Deserialize)]
struct RegisterServiceInfo {
    route_prefix: String,
    port: u16,
}

async fn handle_registry(
    req: Request<hyper::body::Incoming>,
    ip_address: IpAddr,
    services: Arc<RwLock<HashMap<String, Service>>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    println!("{:?}", ip_address);
    let mut response = Response::builder().body(Full::new(Bytes::new())).unwrap();

    if req.uri().path() == "/register" {
        println!("{ip_address}");
        if let Some(payload) = get_payload(req.into_body()).await {
            let mut _services = services.write().await;
            let new_server_address = format!("{}:{}", ip_address, payload.port);

            if let Some(new_service) = _services.get_mut(&payload.route_prefix) {
                new_service.add_instance_server(String::from(new_server_address));
                println!("Servicio actualizado: {}", &payload.route_prefix);
            } else {
                let mut new_service = Service::new();
                new_service.add_instance_server(String::from(new_server_address));
                _services.insert(String::from(&payload.route_prefix), new_service);
                println!("Servicio registrado: {}", &payload.route_prefix);
            }
            *response.status_mut() = StatusCode::ACCEPTED;
        } else {
            println!("Error en la solicitud");
            *response.status_mut() = StatusCode::NOT_FOUND;
        }
    } else {
        *response.status_mut() = StatusCode::NOT_FOUND;
    }

    Ok(response)
}

async fn get_payload(req_body: hyper::body::Incoming) -> Option<RegisterServiceInfo> {
    if let Ok(collected) = req_body.collect().await {
        let reader = collected.aggregate().reader();
        if let Ok(payload) = serde_json::from_reader::<_, RegisterServiceInfo>(reader) {
            Some(payload)
        } else {
            None
        }
    } else {
        None
    }
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

fn get_available_services() -> HashMap<String, Service> {
    let mut services_map: HashMap<String, Service> = HashMap::new();
    let mut new_service = Service::new();

    new_service.add_instance_server(String::from("192.168.100.10"));
    new_service.add_instance_server(String::from("192.168.100.20"));
    new_service.add_instance_server(String::from("192.168.100.30"));
    new_service.add_instance_server(String::from("192.168.100.40"));

    services_map.insert(String::from("/users"), new_service);
    services_map
}

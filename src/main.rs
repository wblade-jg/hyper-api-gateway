use http_body_util::Full;
use hyper::{Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use round_robin::RoundRobin;
use server_group::ServerGroup;
use std::collections::HashMap;
use std::sync::Arc;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpListener;

mod load_balancing;
mod round_robin;
mod server_group;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const CLIENTS_PORT: u16 = 8080;
    let clients_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), CLIENTS_PORT);
    let clients_listener = TcpListener::bind(clients_socket).await?;

    println!("Escuchando clientes en el puerto: {CLIENTS_PORT}");

    let servers = Arc::new(get_servers());

    tokio::spawn(async move { start_server_registry().await.unwrap() });

    start_client_proxy_server(clients_listener, servers).await?;

    Ok(())
}

async fn start_server_registry() -> Result<(), Box<dyn std::error::Error>> {
    const REGISTRY_PORT: u16 = 8500;
    let registry_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), REGISTRY_PORT);
    let registry_listener = TcpListener::bind(registry_socket).await?;

    println!("Server registry escuchando en el puerto: {REGISTRY_PORT}");

    loop {
        let (stream, server_address) = registry_listener.accept().await?;
        let io_stream = TokioIo::new(stream);
        tokio::spawn(async move {
            if let Err(_) = http1::Builder::new()
                .serve_connection(io_stream, service_fn(|req| handle_registry(req, server_address)))
                .await
            {
                println!("Error sirviendo conexion en el service registry");
            }
        });
    }
}

async fn handle_registry(
    req: Request<hyper::body::Incoming>,
    address: SocketAddr
) -> Result<Response<Full<Bytes>>, Infallible> {
    println!("{:?}", address);
    if req.uri().path() == "/register"{
        println!("{address}");
    }
    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

async fn start_client_proxy_server(
    listener: TcpListener,
    servers: Arc<HashMap<String, ServerGroup>>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (stream, _) = listener.accept().await?;
        let io_stream = TokioIo::new(stream);
        let server_clone = Arc::clone(&servers);

        tokio::spawn(async move {
            if let Err(_) = http1::Builder::new()
                .serve_connection(
                    io_stream,
                    service_fn(|req| handle_request(req, Arc::clone(&server_clone))),
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
    servers: Arc<HashMap<String, ServerGroup>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let cuerpo;
    if let Some(server_group) = servers.get(req.uri().path()) {
        let ip_server = server_group.get_next_server();
        cuerpo = ip_server;
    } else {
        cuerpo = String::from("No server");
    }
    Ok(Response::new(Full::from(Bytes::from(cuerpo))))
}

fn get_servers() -> HashMap<String, ServerGroup> {
    let mut servers: HashMap<String, ServerGroup> = HashMap::new();
    let load_balancer = Box::new(RoundRobin::new());
    let mut server_group = ServerGroup::new(load_balancer);

    server_group.add_server(String::from("192.168.100.10"));
    server_group.add_server(String::from("192.168.100.20"));
    server_group.add_server(String::from("192.168.100.30"));
    server_group.add_server(String::from("192.168.100.40"));

    servers.insert(String::from("/users"), server_group);
    servers
}

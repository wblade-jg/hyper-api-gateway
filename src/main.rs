use http_body_util::Full;
use hyper::{Request, Response, body::Bytes, server::conn::http1, service::service_fn};
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
    const PORT: u16 = 8080;

    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT);
    let listener = TcpListener::bind(socket).await?;

    println!("Escuchando en el puerto {PORT}");
    let servers = Arc::new(get_servers());

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

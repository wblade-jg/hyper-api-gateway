use http_body_util::Full;
use hyper::{Request, Response, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use load_balancing::LoadBalancingStrategy;
use round_robin::RoundRobin;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpListener;

mod load_balancing;
mod round_robin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PORT: u16 = 8080;

    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT);
    let listener = TcpListener::bind(socket).await?;

    println!("Escuchando en el puerto {PORT}");
    let servers = Arc::new(get_servers());
    let load_balancer = Arc::new(Mutex::new(RoundRobin::new()));

    loop {
        let (stream, _) = listener.accept().await?;
        let io_stream = TokioIo::new(stream);
        let server_clone = Arc::clone(&servers);
        let load_balancer_clone = Arc::clone(&load_balancer);

        tokio::spawn(async move {
            if let Err(_) = http1::Builder::new()
                .serve_connection(
                    io_stream,
                    service_fn(|req| {
                        handle_request(
                            req,
                            Arc::clone(&server_clone),
                            Arc::clone(&load_balancer_clone),
                        )
                    }),
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
    servers: Arc<HashMap<String, Vec<String>>>,
    load_balancer: Arc<Mutex<impl LoadBalancingStrategy>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let cuerpo;
    if let Some(server_to_route) = servers.get(req.uri().path()) {
        let mut balancer = load_balancer.lock().unwrap();
        balancer.num_servers(server_to_route.len());
        cuerpo = String::from(&server_to_route[balancer.get_next_index()]);
    } else {
        cuerpo = String::from("No server");
    }
    Ok(Response::new(Full::from(Bytes::from(cuerpo))))
}

fn get_servers() -> HashMap<String, Vec<String>> {
    let mut servers: HashMap<String, Vec<String>> = HashMap::new();
    servers.insert(
        String::from("/users"),
        vec![
            String::from("192.168.9.10"),
            String::from("192.168.9.20"),
            String::from("192.168.9.30"),
            String::from("192.168.9.40"),
        ],
    );
    servers
}

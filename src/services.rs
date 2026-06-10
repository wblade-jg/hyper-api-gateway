use crate::{load_balancing::LoadBalancingStrategy, round_robin::RoundRobin};
use http_body_util::{BodyExt, Full};
use hyper::body::Buf;
use hyper::{Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::time::Instant;

pub struct Service {
    servers_list: Vec<Box<ServerInstance>>,
    load_balancer: Mutex<Box<dyn LoadBalancingStrategy + Send>>,
}

pub struct ServerInstance {
    ip_addr: String,
    port: u16,
    last_ping: Mutex<Instant>,
}

impl ServerInstance {
    pub fn new(ip_addr: String) -> Box<Self> {
        let sock_addr: SocketAddr = ip_addr.parse().unwrap();
        Box::new(Self {
            ip_addr: sock_addr.ip().to_string(),
            port: sock_addr.port(),
            last_ping: Mutex::new(Instant::now()),
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn last_ping(&self, new_last_ping: Instant) {
        let mut last_ping = self.last_ping.lock().unwrap();
        *last_ping = new_last_ping;
    }
}

#[derive(Deserialize)]
struct RegisterServiceInfo {
    route_prefix: String,
    port: u16,
}

pub struct ServiceRegistry {
    port: u16,
    services_map: Arc<RwLock<HashMap<String, Service>>>,
}

impl Service {
    pub fn new() -> Self {
        Self {
            servers_list: Vec::new(),
            load_balancer: Mutex::new(Box::new(RoundRobin::new())),
        }
    }

    pub fn add_instance_server(&mut self, sock_addr: String) {
        self.servers_list.push(ServerInstance::new(sock_addr));
    }

    pub fn get_server_instance_to_send(&self) -> String {
        let mut balancer = self.load_balancer.lock().unwrap();
        balancer.num_servers(self.servers_list.len());
        let index = balancer.get_next_index();
        String::from(&self.servers_list[index].ip_addr)
    }

    pub fn get_server_from_ip(&self, ip_addr: &str) -> Option<&Box<ServerInstance>> {
        self.servers_list
            .iter()
            .find(|element| element.ip_addr == ip_addr)
    }
}

fn get_available_services() -> HashMap<String, Service> {
    let mut services_map: HashMap<String, Service> = HashMap::new();
    let mut new_service = Service::new();

    new_service.add_instance_server(String::from("192.168.100.10:3000"));
    new_service.add_instance_server(String::from("192.168.100.20:3000"));
    new_service.add_instance_server(String::from("192.168.100.30:3000"));
    new_service.add_instance_server(String::from("192.168.100.40:3000"));

    services_map.insert(String::from("/users"), new_service);
    services_map
}

impl ServiceRegistry {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            services_map: Arc::new(RwLock::new(get_available_services())),
        }
    }

    pub fn get_available_services(&self) -> Arc<RwLock<HashMap<String, Service>>> {
        Arc::clone(&self.services_map)
    }

    pub async fn start_service_registry(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.port);
        let listener = TcpListener::bind(socket).await?;

        println!("Service registry escuchando en el puerto: {}", self.port);

        let registry_map = Arc::clone(&self.services_map);

        loop {
            let (stream, server_address) = listener.accept().await?;
            let io_stream = TokioIo::new(stream);
            let connection_map = Arc::clone(&registry_map);

            tokio::spawn(async move {
                if let Err(_) = http1::Builder::new()
                    .serve_connection(
                        io_stream,
                        service_fn(|req| {
                            Self::handle_registry(
                                req,
                                server_address.ip(),
                                Arc::clone(&connection_map),
                            )
                        }),
                    )
                    .await
                {
                    println!("Error sirviendo conexion en el service registry");
                }
            });
        }
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

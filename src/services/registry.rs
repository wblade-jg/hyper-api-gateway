use crate::services::Service;
use http_body_util::{BodyExt, Full};
use hyper::body::Buf;
use hyper::{Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{Instrument, error, info, info_span, warn};

pub struct ServiceRegistry {
    port: u16,
    services_map: Arc<RwLock<HashMap<String, Service>>>,
}

#[derive(Deserialize)]
struct RegisterServiceInfo {
    route_prefix: String,
    port: u16,
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

        info!("Service Registry activo en el puerto: {}", self.port);

        let registry_map = Arc::clone(&self.services_map);

        loop {
            let (stream, server_address) = listener.accept().await?;
            let io_stream = TokioIo::new(stream);
            let connection_map = Arc::clone(&registry_map);

            let connection_span = info_span!("http_conn", from = %server_address.ip());

            tokio::spawn(
                (async move {
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
                        warn!("Error al servir la conexion");
                    }
                })
                .instrument(connection_span),
            );
        }
    }

    async fn handle_registry(
        req: Request<hyper::body::Incoming>,
        ip_address: IpAddr,
        services: Arc<RwLock<HashMap<String, Service>>>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let mut response = Response::builder().body(Full::new(Bytes::new())).unwrap();

        if req.uri().path() == "/register" {
            if let Some(payload) = get_payload(req.into_body()).await {
                let mut _services = services.write().await;
                let new_server_address = format!("{}:{}", ip_address, payload.port);

                if let Some(new_service) = _services.get_mut(&payload.route_prefix) {
                    new_service.add_instance_server(String::from(&new_server_address));
                    info!(
                        "Instancia agregada a servicio existe: {}, direccion: {}",
                        &payload.route_prefix, &new_server_address
                    );
                } else {
                    let mut new_service = Service::new();
                    new_service.add_instance_server(String::from(&new_server_address));
                    _services.insert(String::from(&payload.route_prefix), new_service);
                    info!(
                        "Nuevo servicio registrado con exito: {}, direccion inicial: {}",
                        &payload.route_prefix, &new_server_address
                    );
                }
                *response.status_mut() = StatusCode::ACCEPTED;
            } else {
                error!("Error al registrar: El cuerpo de la solicitud no es válido");
                *response.status_mut() = StatusCode::NOT_FOUND;
            }
        } else {
            warn!("Acceso a ruta no válida: {}", req.uri().path());
            *response.status_mut() = StatusCode::NOT_FOUND;
        }

        Ok(response)
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

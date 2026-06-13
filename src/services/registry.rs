use crate::services::{ServerInstance, Service};
use http_body_util::{BodyExt, Full};
use hyper::body::Buf;
use hyper::{Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{Instrument, error, info, info_span, warn};

pub struct ServiceRegistry {
    port: u16,
    instances_map: Arc<RwLock<HashMap<u64, Arc<ServerInstance>>>>,
    services_map: Arc<RwLock<HashMap<String, Service>>>,
    id_gen: Arc<AtomicU64>,
}

#[derive(Deserialize)]
struct RegisterServiceInfo {
    route_prefix: String,
    port: u16,
}

impl ServiceRegistry {
    pub fn new(port: u16) -> Self {
        let id = Arc::new(AtomicU64::new(0));
        let mut instances = HashMap::new();
        let mut services = HashMap::new();
        init_mock(Arc::clone(&id), &mut services, &mut instances);
        Self {
            port,
            instances_map: Arc::new(RwLock::new(instances)),
            id_gen: Arc::clone(&id),
            services_map: Arc::new(RwLock::new(services)),
        }
    }

    pub fn get_available_services(&self) -> Arc<RwLock<HashMap<String, Service>>> {
        Arc::clone(&self.services_map)
    }

    pub fn get_all_instances(&self) -> Arc<RwLock<HashMap<u64, Arc<ServerInstance>>>> {
        Arc::clone(&self.instances_map)
    }

    pub async fn start_service_registry(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.port);
        let listener = TcpListener::bind(socket).await?;

        info!("Service Registry activo en el puerto: {}", self.port);

        let services_map = Arc::clone(&self.services_map);
        let instances_map = Arc::clone(&self.instances_map);
        let id_gen = Arc::clone(&self.id_gen);

        loop {
            let (stream, server_address) = listener.accept().await?;
            let io_stream = TokioIo::new(stream);
            let _services_map = Arc::clone(&services_map);
            let _instances_map = Arc::clone(&instances_map);
            let _id_gen = Arc::clone(&id_gen);

            let connection_span = info_span!("http_conn", from = %server_address.ip());

            tokio::spawn(
                (async move {
                    if http1::Builder::new()
                        .serve_connection(
                            io_stream,
                            service_fn(|req| {
                                Self::handle_registry(
                                    req,
                                    server_address.ip(),
                                    Arc::clone(&_services_map),
                                    Arc::clone(&_instances_map),
                                    Arc::clone(&_id_gen),
                                )
                            }),
                        )
                        .await
                        .is_err()
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
        instances: Arc<RwLock<HashMap<u64, Arc<ServerInstance>>>>,
        id_gen: Arc<AtomicU64>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        if req.uri().path() != "/register" {
            warn!("Acceso a ruta no válida: {}", req.uri().path());
            let mut response = Response::new(Full::new(Bytes::from("Ruta no encontrada")));
            *response.status_mut() = StatusCode::NOT_FOUND;
            return Ok(response);
        }

        if let Some(payload) = get_payload(req.into_body()).await {
            let mut _services = services.write().await;
            let mut _instances = instances.write().await;
            let id = id_gen.fetch_add(1, Ordering::SeqCst);

            let new_server_instance = Arc::new(ServerInstance::new(
                id,
                String::from(&payload.route_prefix),
                ip_address.to_string(),
                payload.port,
            ));

            if let Some(service) = _services.get_mut(&payload.route_prefix) {
                service.add_instance_server(Arc::clone(&new_server_instance));
                _instances.insert(id, new_server_instance);

                info!(
                    "Instancia [{}] agregada a servicio existe: {}, direccion: {}",
                    id,
                    &payload.route_prefix,
                    format!("{}:{}", ip_address.to_string(), payload.port)
                );
            } else {
                let mut new_service = Service::new();
                new_service.add_instance_server(Arc::clone(&new_server_instance));
                _instances.insert(id, new_server_instance);
                _services.insert(payload.route_prefix.clone(), new_service);

                info!(
                    "Nuevo servicio registrado [{}] con exito: {}, direccion inicial: {}",
                    id,
                    &payload.route_prefix,
                    format!("{}:{}", ip_address.to_string(), payload.port)
                );
            }
            let json_response = serde_json::json!({
                "status": "registered",
                "instance_id": id
            });

            let bytes_body = Bytes::from(json_response.to_string());

            let response = Response::builder()
                .status(StatusCode::ACCEPTED)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes_body))
                .unwrap();

            Ok(response)
        } else {
            error!("Error al registrar: El cuerpo de la solicitud no es válido");
            let mut response =
                Response::new(Full::new(Bytes::from("Bad Request: Invalid Payload")));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            Ok(response)
        }
    }
}

fn init_mock(
    id_gen: Arc<AtomicU64>,
    services_map: &mut HashMap<String, Service>,
    instances_map: &mut HashMap<u64, Arc<ServerInstance>>,
) {
    let mut new_service = Service::new();

    let route = String::from("/users");

    let i1 = Arc::new(ServerInstance::new(
        id_gen.fetch_add(1, Ordering::SeqCst),
        route.clone(),
        String::from("192.168.100.10"),
        3000,
    ));
    new_service.add_instance_server(i1.clone());
    instances_map.insert(i1.id(), i1);

    let i2 = Arc::new(ServerInstance::new(
        id_gen.fetch_add(1, Ordering::SeqCst),
        route.clone(),
        String::from("192.168.100.20"),
        3000,
    ));
    new_service.add_instance_server(i2.clone());
    instances_map.insert(i2.id(), i2);

    let i3 = Arc::new(ServerInstance::new(
        id_gen.fetch_add(1, Ordering::SeqCst),
        route.clone(),
        String::from("192.168.100.30"),
        3000,
    ));
    new_service.add_instance_server(i3.clone());
    instances_map.insert(i3.id(), i3);

    let i4 = Arc::new(ServerInstance::new(
        id_gen.fetch_add(1, Ordering::SeqCst),
        route.clone(),
        String::from("192.168.100.40"),
        3000,
    ));
    new_service.add_instance_server(i4.clone());
    instances_map.insert(i4.id(), i4);

    services_map.insert(route, new_service);
}

async fn get_payload(req_body: hyper::body::Incoming) -> Option<RegisterServiceInfo> {
    if let Ok(collected) = req_body.collect().await {
        let reader = collected.aggregate().reader();
        serde_json::from_reader::<_, RegisterServiceInfo>(reader).ok()
    } else {
        None
    }
}

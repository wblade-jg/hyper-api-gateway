use crate::services::{HealthChecker, ServerInstance, Service, ServiceRegistry};
use http_body_util::Full;
use hyper::{Request, Response, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::time::{Instant, Interval, MissedTickBehavior, interval};
use tracing::{Instrument, Level, error, info, info_span, instrument, warn};

mod load_balancing;
mod round_robin;
mod services;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(Level::INFO)
        .init();

    const PORT: u16 = 8080;
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT);
    let listener = TcpListener::bind(socket).await?;

    info!(
        "Api Gateway activo. Escuchando clientes en el puerto: {}",
        PORT
    );

    let service_registry = ServiceRegistry::new(8500);
    let services_map = service_registry.get_available_services();
    let instances = service_registry.get_all_instances();

    tokio::spawn(
        (async move {
            if let Err(e) = service_registry.start_service_registry().await {
                error!("Error crítico en el Service Registry: {e}");
            }
        })
        .instrument(info_span!("registry_thread")),
    );

    let health_checker = HealthChecker::new(8505, Arc::clone(&instances));

    tokio::spawn(
        (async move {
            if let Err(e) = health_checker.start_heath_checker().await {
                error!("Error crítico en el health checker: {e}");
            }
        })
        .instrument(info_span!("health_checker_thread")),
    );

    tokio::spawn(
        (async move {
            let mut frequency = interval(Duration::from_secs(5));
            frequency.set_missed_tick_behavior(MissedTickBehavior::Skip);

            if let Err(e) = start_instance_cleaner_thread(frequency, Arc::clone(&instances)).await {
                error!("Error en el Instance cleaner: {e}");
            }
        })
        .instrument(info_span!("instance_cleaner")),
    );

    start_client_proxy_server(listener, services_map).await?;

    Ok(())
}

async fn start_instance_cleaner_thread(
    mut frequency: Interval,
    instances: Arc<RwLock<HashMap<u64, Arc<ServerInstance>>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        frequency.tick().await;

        let time_now = Instant::now();
        let max_wait = Duration::from_secs(10);

        let server_instances: Vec<Arc<ServerInstance>>;
        { 
            let _instances = instances.read().await; 
            server_instances = _instances.values().cloned().collect();
        }
        
        let mut server_instances_down = Vec::<Arc<ServerInstance>>::new();

        for instance in server_instances{
            if (time_now - instance.last_ping().await) > max_wait{
                server_instances_down.push(instance);
            }
        }

        if !server_instances_down.is_empty() {
            let mut _instances = instances.write().await;
            for instance in server_instances_down {
                _instances.remove(&instance.id());
                info!(
                    id = instance.id(),
                    service = %instance.service_belongs(),
                    address = %instance.socket_addr(),
                    "Instancia inactiva removida"
                );
            }
        }
    }
}

async fn start_client_proxy_server(
    listener: TcpListener,
    services_reference: Arc<RwLock<HashMap<String, Service>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (stream, client_addr) = listener.accept().await?;
        let io_stream = TokioIo::new(stream);
        let services = Arc::clone(&services_reference);

        let connection_span = info_span!("http_conn", cliente = %client_addr);

        tokio::spawn(
            (async move {
                if http1::Builder::new()
                .serve_connection(
                    io_stream,
                    service_fn(|req| {
                        let req_span =
                            info_span!("request", metodo = %req.method(), ruta = %req.uri().path());
                        handle_request(req, Arc::clone(&services)).instrument(req_span)
                    }),
                )
                .await.is_err()
            {
                warn!("Error al servir la conexion");
            }
            })
            .instrument(connection_span),
        );
    }
}

#[instrument(skip_all)]
async fn handle_request(
    req: Request<hyper::body::Incoming>,
    services: Arc<RwLock<HashMap<String, Service>>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let res_body;
    let _services = services.read().await;

    info!("Procesando ruta solicitada por el cliente");

    if let Some(service_to_route) = _services.get(req.uri().path()) {
        let ip_server = service_to_route.get_server_instance_to_send().await;
        info!(target_server = %ip_server, "Redirección exitosa al backend");
        res_body = ip_server;
    } else {
        warn!("Ruta solicitada no coincide con ningún servicio");
        res_body = String::from("No server");
    }

    Ok(Response::new(Full::from(Bytes::from(res_body))))
}

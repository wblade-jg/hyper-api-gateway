use crate::services::{HealthChecker, Service, ServiceRegistry};
use http_body_util::Full;
use hyper::{Request, Response, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::sync::Arc;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
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
    let instances_map_for_health_checker = service_registry.get_all_instances();

    tokio::spawn(
        (async move {
            if let Err(e) = service_registry.start_service_registry().await {
                error!("Error crítico en el Service Registry: {e}");
            }
        })
        .instrument(info_span!("registry_thread")),
    );

    let health_checker = HealthChecker::new(8505, instances_map_for_health_checker);

    tokio::spawn(
        (async move {
            if let Err(e) = health_checker.start_heath_checker().await {
                error!("Error crítico en el health checker: {e}");
            }
        })
        .instrument(info_span!("health_checker_thread")),
    );

    start_client_proxy_server(listener, services_map).await?;

    Ok(())
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

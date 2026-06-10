use crate::services::Service;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{Instrument, info, info_span, warn};

pub struct HealthChecker {
    port: u16,
    services_map: Arc<RwLock<HashMap<String, Service>>>,
}

impl HealthChecker {
    pub fn new(port: u16, services_map: Arc<RwLock<HashMap<String, Service>>>) -> Self {
        Self { port, services_map }
    }

    pub async fn start_heath_checker(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.port);
        let socket = UdpSocket::bind(socket_addr).await?;
        let mut buffer = [0; 1024];

        info!("Health checker activo en el puerto: {}", self.port);

        let services = Arc::clone(&self.services_map);

        loop {
            let (len, addr) = socket.recv_from(&mut buffer).await?;
            let datos_recibidos = &buffer[..len];

            if let Ok(mensaje) = std::str::from_utf8(datos_recibidos) {
                let mensaje = mensaje.trim();
                let ping_span = info_span!("udp_ping", servicio = %mensaje, cliente = %addr.ip());
                let mut _services = services.read().await;
                async {
                    if let Some(service) = _services.get(mensaje) {
                        if let Some(server) = service.get_server_from_ip(&addr.ip().to_string()) {
                            server.last_ping(Instant::now()).await;
                            info!("Heartbeat registrado");
                        } else {
                            warn!("El servidor emisor no está registrado en este servicio");
                        }
                    } else {
                        warn!("Intento de ping de un servicio no configurado");
                    }
                }
                .instrument(ping_span)
                .await;
            } else {
                warn!(from = %addr.ip(), "No se pudo parsear el paquete UDP a UTF-8");
            }
        }
    }
}

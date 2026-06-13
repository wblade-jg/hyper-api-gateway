use crate::services::ServerInstance;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{Instrument, info, info_span, warn};

pub struct HealthChecker {
    port: u16,
    instances_map: Arc<RwLock<HashMap<u64, Arc<ServerInstance>>>>,
}

#[derive(Deserialize)]
struct HeartbeatPayload {
    id: u64,
}

impl HealthChecker {
    pub fn new(port: u16, instances_map: Arc<RwLock<HashMap<u64, Arc<ServerInstance>>>>) -> Self {
        Self {
            port,
            instances_map,
        }
    }

    pub async fn start_heath_checker(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.port);
        let socket = UdpSocket::bind(socket_addr).await?;
        let mut buffer = [0; 1024];

        info!("Health checker activo en el puerto: {}", self.port);

        let instances = Arc::clone(&self.instances_map);

        loop {
            let (len, addr) = socket.recv_from(&mut buffer).await?;
            let datos_recibidos = &buffer[..len];

            if let Ok(payload) = serde_json::from_slice::<HeartbeatPayload>(datos_recibidos) {
                let id = payload.id;
                let ping_span = info_span!("udp_ping", cliente = %addr.ip());
                let instance = {
                    let _instances = instances.read().await;
                    _instances.get(&id).cloned()
                }; //Lo hacemos en un bloque para no bloquear más tiempo del necesario
                
                let ack = format!("{{\"id\": {} }}", id).into_bytes();

                async {
                    if let Some(server_instance) = instance {
                        server_instance.update_last_ping(Instant::now()).await;
                        info!("Heartbeat registrado. Instancia [{}]: {}", id, server_instance.socket_addr());
                        let _ = socket.send_to(&ack, addr).await;
                    } else {
                        warn!("Intento de ping de una instancia no registrada: [{}]", id);
                    }
                }
                .instrument(ping_span)
                .await;
            } else {
                warn!(from = %addr.ip(), "No se pudo parsear el paquete UDP");
            }
        }
    }
}

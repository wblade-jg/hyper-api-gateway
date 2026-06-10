use std::net::SocketAddr;
use tokio::sync::Mutex;
use tokio::time::Instant;

pub struct ServerInstance {
    ip_addr: String,
    port: u16,
    last_ping: Mutex<Instant>,
}

impl ServerInstance {
    pub fn new(ip_addr: String) -> Self {
        let sock_addr: SocketAddr = ip_addr.parse().unwrap();
        Self {
            ip_addr: sock_addr.ip().to_string(),
            port: sock_addr.port(),
            last_ping: Mutex::new(Instant::now()),
        }
    }
    
    pub fn ip_addr(&self) -> String{
        String::from(&self.ip_addr)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn last_ping(&self, new_last_ping: Instant) {
        let mut last_ping = self.last_ping.lock().await;
        *last_ping = new_last_ping;
    }
}

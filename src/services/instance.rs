use tokio::sync::Mutex;
use tokio::time::Instant;

pub struct ServerInstance {
    ip_addr: String,
    port: u16,
    last_ping: Mutex<Instant>,
}

impl ServerInstance {
    pub fn new(ip_addr: String, port: u16) -> Self {
        Self {
            ip_addr, 
            port, 
            last_ping: Mutex::new(Instant::now()),
        }
    }
    
    pub fn socket_addr(&self) -> String{
        format!("{}:{}", self.ip_addr, self.port)
    }

    pub async fn last_ping(&self, new_last_ping: Instant) {
        let mut last_ping = self.last_ping.lock().await;
        *last_ping = new_last_ping;
    }
}

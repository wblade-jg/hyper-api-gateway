use tokio::sync::Mutex;
use tokio::time::Instant;

pub struct ServerInstance {
    id: u64,
    service_name: String,
    ip_addr: String,
    port: u16,
    last_ping: Mutex<Instant>,
}

impl ServerInstance {
    pub fn new(id: u64, service_name: String, ip_addr: String, port: u16) -> Self {
        Self {
            id,
            service_name,
            ip_addr, 
            port, 
            last_ping: Mutex::new(Instant::now()),
        }
    }
    
    pub fn id(&self) -> u64{
        self.id
    }

    pub fn service_belongs(&self) -> String{
        self.service_name.clone()
    }

    pub fn socket_addr(&self) -> String{
        format!("{}:{}", self.ip_addr, self.port)
    }

    pub async fn last_ping(&self) -> Instant{
        let value = self.last_ping.lock().await;
        *value
    }

    pub async fn update_last_ping(&self, new_last_ping: Instant) {
        let mut last_ping = self.last_ping.lock().await;
        *last_ping = new_last_ping;
    }
}

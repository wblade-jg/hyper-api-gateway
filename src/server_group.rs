use crate::load_balancing::LoadBalancingStrategy;
use std::sync::Mutex;

pub struct ServerGroup {
    servers: Vec<String>,
    load_balancer: Mutex<Box<dyn LoadBalancingStrategy + Send>>,
}

impl ServerGroup {
    pub fn new(load_balancer: Box<dyn LoadBalancingStrategy + Send>) -> Self {
        Self {
            servers: Vec::new(),
            load_balancer: Mutex::new(load_balancer),
        }
    }

    pub fn add_server(&mut self, ip_server: String) {
        self.servers.push(ip_server);
    }

    pub fn get_next_server(&self) -> String {
        let mut balancer = self.load_balancer.lock().unwrap();
        balancer.num_servers(self.servers.len());
        let index = balancer.get_next_index();
        String::from(&self.servers[index])
    }
}

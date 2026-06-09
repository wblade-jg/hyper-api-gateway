use crate::{load_balancing::LoadBalancingStrategy, round_robin::RoundRobin};
use std::sync::Mutex;

pub struct Service {
    servers_list: Vec<String>,
    load_balancer: Mutex<Box<dyn LoadBalancingStrategy + Send>>,
}

impl Service {
    pub fn new() -> Self {
        Self {
            servers_list: Vec::new(),
            load_balancer: Mutex::new(Box::new(RoundRobin::new())),
        }
    }

    pub fn add_instance_server(&mut self, ip_server: String) {
        self.servers_list.push(ip_server);
    }

    pub fn get_server_instance_to_send(&self) -> String {
        let mut balancer = self.load_balancer.lock().unwrap();
        balancer.num_servers(self.servers_list.len());
        let index = balancer.get_next_index();
        String::from(&self.servers_list[index])
    }
}

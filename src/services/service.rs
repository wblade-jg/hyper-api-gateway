use crate::services::ServerInstance;
use crate::{load_balancing::LoadBalancingStrategy, round_robin::RoundRobin};
use tokio::sync::Mutex;

pub struct Service {
    servers_list: Vec<ServerInstance>,
    load_balancer: Mutex<Box<dyn LoadBalancingStrategy + Send>>,
}

impl Service {
    pub fn new() -> Self {
        Self {
            servers_list: Vec::new(),
            load_balancer: Mutex::new(Box::new(RoundRobin::new())),
        }
    }
    
    pub fn add_instance_server(&mut self, sock_addr: String) {
        self.servers_list.push(ServerInstance::new(sock_addr));
    }

    pub async fn get_server_instance_to_send(&self) -> String {
        let mut balancer = self.load_balancer.lock().await;
        balancer.num_servers(self.servers_list.len());
        let index = balancer.get_next_index();
        self.servers_list[index].ip_addr()
    }

    pub fn get_server_from_ip(&self, ip_addr: &str) -> Option<&ServerInstance> {
        self.servers_list
            .iter()
            .find(|element| element.ip_addr() == ip_addr)
    }
}


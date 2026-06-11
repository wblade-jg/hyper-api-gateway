use crate::services::ServerInstance;
use crate::{load_balancing::LoadBalancingStrategy, round_robin::RoundRobin};
use tokio::sync::Mutex;
use std::sync::Arc;

pub struct Service {
    servers_list: Vec<Arc<ServerInstance>>,
    load_balancer: Mutex<Box<dyn LoadBalancingStrategy + Send>>,
}

impl Service {
    pub fn new() -> Self {
        Self {
            servers_list: Vec::new(),
            load_balancer: Mutex::new(Box::new(RoundRobin::new())),
        }
    }
    
    pub fn add_instance_server(&mut self, server_instance: Arc<ServerInstance>){
        self.servers_list.push(server_instance);
    }

    pub async fn get_server_instance_to_send(&self) -> String {
        let mut balancer = self.load_balancer.lock().await;
        balancer.num_servers(self.servers_list.len());
        let index = balancer.get_next_index();
        self.servers_list[index].socket_addr()
    }
}


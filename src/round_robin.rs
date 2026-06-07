use crate::load_balancing::LoadBalancingStrategy;

pub struct RoundRobin {
    next_index: usize,
    num_servers: usize,
}

impl LoadBalancingStrategy for RoundRobin {
    fn get_next_index(&mut self) -> usize {
        self.next_index = (self.next_index + 1) % self.num_servers;
        self.next_index
    }

    fn num_servers(&mut self, num_servers: usize) {
        self.num_servers = num_servers;
    }
}

impl RoundRobin {
    pub fn new() -> Self {
        Self {
            next_index: 0,
            num_servers: 0,
        }
    }
}

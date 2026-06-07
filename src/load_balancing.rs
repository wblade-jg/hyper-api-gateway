pub trait LoadBalancingStrategy {
    fn get_next_index(&mut self) -> usize;
    fn num_servers(&mut self, num_servers: usize);
}

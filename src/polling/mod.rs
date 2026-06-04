use crate::context::Ctx;
use crate::nodes::Node;
mod poll_local;
mod poll_slurm;
pub use poll_local::LocalPoller;
pub use poll_slurm::SlurmPoller;

pub trait Poller {
    fn poll(&mut self, nodes: &[Node], ctx: &mut Ctx);
}

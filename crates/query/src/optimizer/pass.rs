//! Optimizer pass trait.

use crate::planner::LogicalPlan;

/// An optimization pass that transforms a logical plan.
pub trait OptimizerPass {
    /// Optimizes the given logical plan.
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan;

    /// Returns the name of this pass.
    fn name(&self) -> &'static str {
        "unnamed"
    }
}

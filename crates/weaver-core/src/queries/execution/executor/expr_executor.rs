use crate::error::WeaverError;
use crate::queries::query_plan::QueryPlan;

#[derive(Debug)]
pub struct ExpressionEvaluator {
}

impl ExpressionEvaluator {

    /// Compiles an expression evaluator from a query plan
    pub fn compile(plan: &QueryPlan) -> Result<Self, WeaverError>{
        Ok(Self {

        })
    }

}
//! Visitors for queries

use crate::ast::{ColumnRef, Expr, FromClause, Identifier, JoinClause, JoinConstraint, Literal, Query, ResolvedColumnRef, ResultColumn, Select, TableOrSubQuery, UnresolvedColumnRef};

/// Creates a mut visitor
#[macro_export]
macro_rules! visit_mut {
    ($($vis:vis visit ($visitor_id:ident, $visited_id:ident: &mut $visited:ty) -> Result<()> $block:block)+) => {
        $(
        paste::paste! {
            $vis fn [<visit_ $visited:snake  _mut>]<V : VisitorMut + ?Sized>($visitor_id: &mut V, $visited_id: &mut $visited) -> std::result::Result<(), V::Err> {
                $block
            }
        }
        )*

        pub trait VisitorMut {
            type Err;

            $(
            paste::paste! {
                fn [<visit_ $visited:snake  _mut>](&mut self, $visited_id: &mut $visited) -> std::result::Result<(), Self::Err> {
                    [<visit_ $visited:snake  _mut>](self, $visited_id)
                }
            }
            )*
        }
    };


}

visit_mut! {
    pub visit (visitor, query: &mut Query) -> Result<()> {
        match query {
                Query::Explain(_) => {
                    todo!("visit explain")
                }
                Query::Select(select) => visitor.visit_select_mut(select),
                Query::QueryList(_) => {
                    todo!("visit query list")
                }
            }
    }
    pub visit (visitor, select: &mut Select) -> Result<()> {
        let Select {
            columns,
            from,
            condition,
            limit,
            offset,
        } = select;

        columns.iter_mut().try_for_each(|col| {
            visitor.visit_result_column_mut(col)
        })?;

        if let Some(from) = from {
            visitor.visit_from_clause_mut(from)?;
        }

        if let Some(from) = condition {
            visitor.visit_expr_mut(from)?;
        }


        Ok(())
    }

    pub visit (visitor, from: &mut FromClause) -> Result<()> {
        visitor.visit_table_or_sub_query_mut(&mut from.0)
    }

    pub visit (visitor, table_or_sub_query: &mut TableOrSubQuery) -> Result<()> {
        match table_or_sub_query {
            TableOrSubQuery::Table{
                schema,
                table_name,
                alias } => {
                if let Some(schema) = schema {
                    visitor.visit_identifier_mut(schema)?;
                }
                visitor.visit_identifier_mut(table_name)?;
                if let Some(alias) = alias {
                    visitor.visit_identifier_mut(alias)?;
                }
                Ok(())
            }
            TableOrSubQuery::Select{ select, alias } => {
                visitor.visit_select_mut(select)?;
                if let Some(alias) = alias {
                    visitor.visit_identifier_mut(alias)?;
                }
                Ok(())
            }
            TableOrSubQuery::Multiple(mult) => {
                mult.iter_mut().try_for_each(|tbq| visitor.visit_table_or_sub_query_mut(tbq))
            }
            TableOrSubQuery::JoinClause(join_clause) => {
                visitor.visit_join_clause_mut(join_clause)
            }
        }
    }

    pub visit (visitor, join_clause: &mut JoinClause) -> Result<()> {
        let JoinClause {
            left,
            op,
            right,
            constraint } = join_clause;

        visitor.visit_table_or_sub_query_mut(left)?;
        visitor.visit_table_or_sub_query_mut(right)?;
        visitor.visit_join_constraint_mut(constraint)?;

        Ok(())
    }

    pub visit (visitor, join_constraint: &mut JoinConstraint) -> Result<()> {
        let JoinConstraint { on } = join_constraint;
        visitor.visit_expr_mut(on)
    }

    pub visit (visitor, result_column: &mut ResultColumn) -> Result<()> {
        match result_column {
            ResultColumn::Wildcard => {
                Ok(())
            }
            ResultColumn::TableWildcard(id) => {
                visitor.visit_identifier_mut(id)
            }
            ResultColumn::Expr{expr, alias  } => {
                visitor.visit_expr_mut(expr)?;
                if let Some(alias) = alias {
                    visitor.visit_identifier_mut(alias)
                } else {
                    Ok(())
                }
            }
        }
    }
    pub visit (_visitor, _id: &mut Identifier) -> Result<()> {
        Ok(())
    }
    pub visit (visitor, expr: &mut Expr) -> Result<()> {
        match expr {
            Expr::Column{ column } => { visitor.visit_column_ref_mut(column) }
            Expr::Literal{ literal } => { visitor.visit_literal_mut(literal)}
            Expr::BindParameter{ .. } => { Ok(())}
            Expr::Unary{ op: _,expr  } => { visitor.visit_expr_mut(expr)}
            Expr::Binary{ left,op: _,right  } => {
                visitor.visit_expr_mut(left)?;
                visitor.visit_expr_mut(right)
            }
        }
    }
    pub visit (visitor, column: &mut ColumnRef) -> Result<()> {
        match column {
            ColumnRef::Resolved(resolved) => visitor.visit_resolved_column_ref_mut(resolved),
            ColumnRef::Unresolved(unresolved) => visitor.visit_unresolved_column_ref_mut(unresolved),
        }
    }
    pub visit (_visitor, _c: &mut ResolvedColumnRef) -> Result<()> {
        Ok(())
    }
    pub visit (_visitor, _c: &mut UnresolvedColumnRef) -> Result<()> {
        Ok(())
    }
    pub visit (_visitor, _literal: &mut Literal) -> Result<()> {
        Ok(())
    }
}

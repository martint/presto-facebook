/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.optimizer;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.optimizer.engine.GreedyOptimizer;
import com.facebook.presto.sql.optimizer.engine.HeuristicPlannerMemo;
import com.facebook.presto.sql.optimizer.tree.Apply;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;
import com.facebook.presto.sql.optimizer.tree.RelationTypeStamp;
import com.facebook.presto.sql.optimizer.tree.TypeStamp;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.sql.optimizer.engine.Patterns.isCall;
import static java.util.stream.Collectors.toList;

public class NewToLegacy
{
    private final HeuristicPlannerMemo memo;
    private final GreedyOptimizer.MemoLookup lookup;

    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;

    public NewToLegacy(HeuristicPlannerMemo memo, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        this.memo = memo;
        this.lookup = new GreedyOptimizer.MemoLookup(memo);
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
    }

    public PlanNode extract()
    {
        return translate(memo.getExpression(memo.getRoot()));
    }

    private PlanNode translate(Expression expression)
    {
        Expression target = lookup.resolve(expression);

        if (isCall(target, "transform", lookup)) {
            Apply apply = (Apply) target;
            PlanNode source = translate(apply.getArguments().get(0));
            Lambda lambda = (Lambda) lookup.resolve(apply.getArguments().get(1));

            // TODO translate lambda body to scalar expressions
            throw new UnsupportedOperationException("not yet implemented: transform");
        }
        else if (isCall(target, "array", lookup)) {
            Apply apply = (Apply) target;

            RelationTypeStamp type = (RelationTypeStamp) apply.type();
            List<Symbol> names = type.getColumns().stream()
                    .map(c -> symbolAllocator.newSymbol("col", translate(c)))
                    .collect(toList());

            ImmutableList.Builder<List<com.facebook.presto.sql.tree.Expression>> rows = ImmutableList.builder();
            for (Expression rowItem : apply.getArguments()) {
                ImmutableList.Builder<com.facebook.presto.sql.tree.Expression> rowBuilder = ImmutableList.builder();
                Apply row = (Apply) lookup.resolve(rowItem);
                for (Expression cell : row.getArguments()) {
                    rowBuilder.add(translateScalar(cell));
                }
                rows.add(rowBuilder.build());
            }

            return new ValuesNode(idAllocator.getNextId(), names, rows.build());
        }

        throw new UnsupportedOperationException("not yet implemented: " + target.getClass().getName() + " => " + target);
    }

    private Type translate(TypeStamp type)
    {
        throw new UnsupportedOperationException("not yet implemented: " + type);
    }

    private com.facebook.presto.sql.tree.Expression translateScalar(Expression expression)
    {
        Expression target = lookup.resolve(expression);

        throw new UnsupportedOperationException("not yet implemented: " + target.getClass().getName() + " => " + target);
    }
}

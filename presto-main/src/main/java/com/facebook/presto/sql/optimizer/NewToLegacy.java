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
import com.facebook.presto.sql.optimizer.tree.sql.Null;
import com.facebook.presto.sql.optimizer.tree.type.RelationTypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.TypeStamp;
import com.facebook.presto.sql.optimizer.tree.type.UnknownTypeStamp;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.facebook.presto.sql.optimizer.engine.Patterns.isCall;
import static com.google.common.base.Preconditions.checkArgument;
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
        return translate(memo.getExpression(memo.getRoot())).getNode();
    }

    private Translation translate(Expression expression)
    {
        Expression target = lookup.resolve(expression);

        if (isCall(target, "transform", lookup)) {
            Apply apply = (Apply) target;
            Translation source = translate(apply.getArguments().get(0));
            Lambda lambda = (Lambda) lookup.resolve(apply.getArguments().get(1));

            // Need map of type in source expression -> symbols in translated PlanNode
            // Need map of lambda scope level -> symbols in scope (for correlations)

            ImmutableList.Builder<Symbol> fieldMappings = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, com.facebook.presto.sql.tree.Expression> assignments = ImmutableMap.builder();

            List<com.facebook.presto.sql.tree.Expression> projections = translateProjections(lookup.resolve(lambda.getBody()));

            RelationTypeStamp type = (RelationTypeStamp) expression.type();
            for (int i = 0; i < projections.size(); i++) {
                Symbol name = symbolAllocator.newSymbol(projections.get(i), translate(type.getRowType().getColumnTypes().get(i)));
                fieldMappings.add(name);
                assignments.put(name, projections.get(i));
            }

            return new Translation(
                    fieldMappings.build(),
                    new ProjectNode(idAllocator.getNextId(), source.getNode(), assignments.build()));
        }
        else if (isCall(target, "array", lookup)) {
            Apply apply = (Apply) target;

            RelationTypeStamp type = (RelationTypeStamp) apply.type();
            List<Symbol> names = type.getRowType().getColumnTypes().stream()
                    .map(columnType -> symbolAllocator.newSymbol("col", translate(columnType)))
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

            return new Translation(
                    names,
                    new ValuesNode(idAllocator.getNextId(), names, rows.build()));
        }

        throw new UnsupportedOperationException("not yet implemented: " + target.getClass().getName() + " => " + target);
    }

    private List<com.facebook.presto.sql.tree.Expression> translateProjections(Expression expression)
    {
        // expects expression to be of RowTypeStamp. Unpacks the fields and returns independent expression for each one
//        checkArgument(expression.type() instanceof RowTypeStamp, "expected an expression of RowTypeStamp type");
        checkArgument(isCall(expression, "row", lookup));

        ImmutableList.Builder<com.facebook.presto.sql.tree.Expression> builder = ImmutableList.builder();

        Apply apply = (Apply) expression;
        for (Expression argument : apply.getArguments()) {
            builder.add(translateScalar(argument));
        }

        return builder.build();
    }

    private Type translate(TypeStamp type)
    {
        if (type instanceof UnknownTypeStamp) {
            return UnknownType.UNKNOWN;
        }

        throw new UnsupportedOperationException("not yet implemented: " + type.getClass().getName() + " (" + type + ")");
    }

    private com.facebook.presto.sql.tree.Expression translateScalar(Expression expression)
    {
        Expression target = lookup.resolve(expression);

        if (target instanceof Null) {
            return new NullLiteral();
        }

        throw new UnsupportedOperationException("not yet implemented: " + target.getClass().getName() + " => " + target);
    }

    private static class Translation
    {
        // which symbol in the node's output does each field in the original relational expression map to
        private final List<Symbol> fieldMapping;
        private final PlanNode node;

        public Translation(List<Symbol> fieldMapping, PlanNode node)
        {
            this.fieldMapping = fieldMapping;
            this.node = node;
        }

        public List<Symbol> getFieldMapping()
        {
            return fieldMapping;
        }

        public PlanNode getNode()
        {
            return node;
        }
    }
}

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
package com.facebook.presto.sql.optimizer.old.engine;

import java.util.List;

import static java.util.Objects.requireNonNull;

class Engine
{
    public Engine(List<Object> rules)
    {
        this.rules = requireNonNull(rules, "rules is null");
    }

    private final List<Object> rules;

//    public void optimize(Expression expression)
//    {
//        Memo memo = new Memo();
//        EquivalenceClass clazz = memo.insert(expression);
//
//        optimizeEquivalenceClass(memo, clazz, Cost.UNBOUNDED, null);
//
//        // TODO: extract optimal expression from memo
//
//        System.out.println("*** Memo ****");
//        System.out.println(memo.dump());
//    }

//    private void optimizeEquivalenceClass(Memo memo, EquivalenceClass equivalenceClass, Cost maximumCost, Requirements requirements)
//    {
//        if (memo.isOptimized(equivalenceClass, requirements)) {
//            return;
//        }

//        exploreEquivalenceClass(equivalenceClass, memo);
//    }

//    private void exploreEquivalenceClass(EquivalenceClass equivalenceClass, Memo memo)
//    {
//        System.out.println(String.format("Explore %s", equivalenceClass));

    // explore expressions
//        Queue<Expression> expressions = new LinkedList<>(memo.getExpressions(equivalenceClass));
//        while (!expressions.isEmpty()) {
//            Expression expression = expressions.poll();

//            for (Object rule : rules) {
//                memo.match(/* TODO: rule.getPattern(), */ expression)
//                        .forEach(match -> {
//                            if (rule.canApply(match)) {
//                                System.out.println(String.format("Apply %s(%s)", rule, match));
//                                for (Expression transformed : rule.apply(match)) {
//                                    System.out.println(String.format("-> %s", transformed));
//                                    // TODO: transformed = memo.recordEquivalence(equivalenceClass, transformed);
//                                    expressions.add(transformed);
//                                }
//                            }
//                        });
//            }
//        }

//        for (Expression expression : memo.getExpressions(equivalenceClass)) {
//            for (Expression input : expression.getArguments()) {
//                exploreEquivalenceClass(memo.getEquivalenceClass(input), memo);
//            }
//        }
//    }

    // TODO: how to re-explore potential matches that my be created by exploring children

//            if (expression.isPhysical()) {
//                optimizeExpression(expression, maximumCost, requirements, memo);
//                // TODO: set maximumCost to cost of optimizedExpression if one was found with a lower cost (helps prune the space)
//            }

    // TODO: explore expression

    // TODO: optimization of children may create new potential matches, so we need to re-try patterns.

//    private void optimizeExpression(Expression expression, Cost maximumCost, Requirements requirements, Memo memo)
//    {
//        for (Expression argument : expression.getArguments()) {
//            Reference reference = (Reference) argument;
//            Requirements inputRequirements = null; // TODO: derive child requirements
//            Cost maximumChildCost = null; // TODO: derive max child cost
//            optimizeEquivalenceClass(memo, memo.getEquivalenceClass(reference), maximumChildCost, inputRequirements);
//
//            // TODO: short-circuit if we can't achieve cost target, but remember the fact that we couldn't
//            // in case another request tries to optimize the same expression+reqs+ a lower cost bound
//        }
//
//        // TODO: derive expression properties from input properties
//        // TODO: add enforcer?
//    }
}

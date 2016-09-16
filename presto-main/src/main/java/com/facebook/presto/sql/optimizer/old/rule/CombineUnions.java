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
package com.facebook.presto.sql.optimizer.old.rule;

import com.facebook.presto.sql.optimizer.old.engine.Lookup;
import com.facebook.presto.sql.optimizer.old.engine.Rule;
import com.facebook.presto.sql.optimizer.old.tree.Expression;
import com.facebook.presto.sql.optimizer.old.tree.Reference;
import com.facebook.presto.sql.optimizer.old.tree.Union;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO: may need a way to "kill" activation of the rule for any nested unions that were discovered and handled by
 * this rule. Otherwise, we end up with a combinatorial explosion of flattenings
 */
public class CombineUnions
        implements Rule
{
    // Experimental: a mechanism to suppress firing for expressions that have
    // already been considered. This should be done by the engine
//    private final Set<Union> skip = new HashSet<>();

    @Override
    public Stream<Expression> apply(Expression expression, Lookup lookup)
    {
//        if (skip.contains(expression)) {
//            return Stream.empty();
//        }
        if (expression instanceof Reference) {
            throw new UnsupportedOperationException("not yet implemented");
        }
        else if (expression instanceof Union) {
            return process(expression, lookup).stream()
                    .filter(args -> !ImmutableSet.copyOf(expression.getArguments()).equals(ImmutableSet.copyOf(args))) // don't produce a result if it matches the expression
                    .map(ArrayList::new)
                    .map(Union::new);
        }

        return Stream.empty();
    }

    private Set<Set<Expression>> process(Expression expression, Lookup lookup)
    {
        // TODO: use functional style
        Set<Set<Expression>> result = new HashSet<>();
        lookup.lookup(expression).
                forEach(member -> {
                    if (member instanceof Union) {
//                        skip.add((Union) member);
                        List<Set<Set<Expression>>> alternatives = new ArrayList<>();
                        for (Expression argument : member.getArguments()) {
                            Set<Set<Expression>> child = process(argument, lookup);
                            alternatives.add(child);
                        }

                        for (List<Set<Expression>> sets : Sets.cartesianProduct(alternatives)) {
                            result.add(sets.stream()
                                    .flatMap(Set::stream)
                                    .collect(Collectors.toSet()));
                        }
                    }
                    else {
                        result.add(ImmutableSet.of(expression));
                    }
                });
        return result;
    }
}

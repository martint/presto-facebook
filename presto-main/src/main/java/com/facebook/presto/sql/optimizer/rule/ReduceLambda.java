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
package com.facebook.presto.sql.optimizer.rule;

import com.facebook.presto.sql.optimizer.engine.Lookup;
import com.facebook.presto.sql.optimizer.engine.Rule;
import com.facebook.presto.sql.optimizer.tree.Apply;
import com.facebook.presto.sql.optimizer.tree.Atom;
import com.facebook.presto.sql.optimizer.tree.Expression;
import com.facebook.presto.sql.optimizer.tree.Lambda;
import com.facebook.presto.sql.optimizer.tree.ScopeReference;
import com.facebook.presto.sql.optimizer.tree.type.LambdaTypeStamp;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.sql.optimizer.tree.Expressions.lambda;
import static com.facebook.presto.sql.optimizer.tree.Expressions.reference;
import static com.google.common.base.Preconditions.checkArgument;

// TODO: tests
// TODO: avoid rewriting tree branches that don't change (may need to return Let expressions)

/**
 * Reduces expressions of the form:
 *
 * (\.t1) t2
 */
public class ReduceLambda
        implements Rule
{
    @Override
    public Stream<Expression> transform(Expression expression, Lookup lookup)
    {
        if (!(expression instanceof Apply)) {
            return Stream.empty();
        }

        Apply apply = (Apply) expression;
        Expression target = lookup.resolve(apply.getTarget());
        if (!(target instanceof Lambda)) {
            return Stream.empty();
        }

        Lambda lambda = (Lambda) target;

        checkArgument(apply.getArguments().size() == 1, "Only one argument currently supported");

        return Stream.of(reduce(lambda, apply.getArguments().get(0), lookup));
    }

    // substitute all occurrences of %0 with argument
    private Expression reduce(Lambda lambda, Expression argument, Lookup lookup)
    {
        return shift(
                substitute(lambda.getBody(), shift(argument, 1, 0, lookup), 0, lookup),
                -1,
                0,
                lookup);
    }

    private Expression substitute(Expression target, Expression replacement, int match, Lookup lookup)
    {
        Expression resolved = lookup.resolve(target);

        if (resolved instanceof ScopeReference && match == ((ScopeReference) resolved).getIndex()) {
            return replacement;
        }

        if (resolved instanceof ScopeReference && match != ((ScopeReference) resolved).getIndex()) {
            return target;
        }

        if (resolved instanceof Lambda) {
            // [t/j] \.u = \.[shift(t, 1, 0)/j+1] u
            return lambda(
                    (LambdaTypeStamp) resolved.type(),
                    substitute(((Lambda) resolved).getBody(), shift(replacement, 1, 0, lookup), match + 1, lookup));
        }

        if (resolved instanceof Apply) {
            Apply apply = (Apply) resolved;
            return new Apply(
                    resolved.type(),
                    substitute(apply.getTarget(), replacement, match, lookup),
                    apply.getArguments().stream()
                            .map(e -> substitute(e, replacement, match, lookup))
                            .collect(Collectors.toList()));
        }

        if (resolved instanceof Atom) {
            return target;
        }

        throw new UnsupportedOperationException("Unsupported expression type: " + target.getClass().getName());
    }

    private Expression shift(Expression expression, int shift, int cutoff, Lookup lookup)
    {
        Expression resolved = lookup.resolve(expression);

        if (resolved instanceof ScopeReference && ((ScopeReference) resolved).getIndex() < cutoff) {
            return expression;
        }

        if (resolved instanceof ScopeReference && ((ScopeReference) resolved).getIndex() >= cutoff) {
            return reference(resolved.type(), ((ScopeReference) resolved).getIndex() + shift);
        }

        if (resolved instanceof Lambda) {
            return lambda(
                    (LambdaTypeStamp) resolved.type(),
                    shift(((Lambda) resolved).getBody(), shift, cutoff + 1, lookup));
        }

        if (resolved instanceof Apply) {
            Apply apply = (Apply) resolved;
            return new Apply(
                    apply.type(),
                    shift(apply.getTarget(), shift, cutoff, lookup),
                    apply.getArguments().stream()
                            .map(e -> shift(e, shift, cutoff, lookup))
                            .collect(Collectors.toList()));
        }

        if (resolved instanceof Atom) {
            return expression;
        }

        throw new UnsupportedOperationException("Unsupported expression type: " + resolved.getClass().getName());
    }
}

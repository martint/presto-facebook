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
package com.facebook.presto.sql.newplanner;

import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.QualifiedName;

import java.util.HashMap;
import java.util.Map;

/*

    - nested scopes for resolving fields and relational exprs
    - binding fields to in outer scope to variables when referenced
    - mapping sub-exprs to fields as relational expressions are stacked

    - resolution of fields in joins?
 */
public class Scope
{
    // parent scope
    private final Scope parent;

    // CTEs, expanded views, etc.
    private final Map<String, TranslatedRelationalExpression> namedExpressions = new HashMap<>();

    // Available fields in the input expression
    private final TupleDescriptor fields;

    public Scope()
    {
        parent = null;
        fields = null;
    }

    public Scope(Scope parent)
    {
        this.parent = parent;
        fields = null;
    }

    public Scope(Scope parent, TupleDescriptor fields)
    {
        this.parent = parent;
        this.fields = fields;
    }

    public Scope getParent()
    {
        return parent;
    }

    public TupleDescriptor getTupleDescriptor()
    {
        return fields;
    }

    // mapping of relation names (e.g., CTE) -> relational expressions
    public RelationalExpression get(QualifiedName name)
    {
        return null;
    }

    public void register(String name, TranslatedRelationalExpression expression)
    {
    }
}

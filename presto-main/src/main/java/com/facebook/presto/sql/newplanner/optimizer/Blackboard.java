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
package com.facebook.presto.sql.newplanner.optimizer;

import com.facebook.presto.sql.newplanner.expression.EquivalenceGroupReferenceExpression;
import com.facebook.presto.sql.newplanner.expression.RelationalExpression;
import com.facebook.presto.sql.tree.Relation;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Blackboard
{
    private final Map<Integer, EquivalenceGroup> equivalenceGroups = new HashMap<>();
    private final int rootGroup;

    private int nextId;

    public Blackboard(RelationalExpression initial)
    {
        rootGroup = addRecursive(initial);
    }

    public List<RelationalExpression> getExpressionsInGroup(int group)
    {
        return equivalenceGroups.get(group).getExpressions();
    }

    private int addRecursive(RelationalExpression expression)
    {
        List<RelationalExpression> references = new ArrayList<>();
        for (RelationalExpression input : expression.getInputs()) {
            references.add(new EquivalenceGroupReferenceExpression(nextId++, addRecursive(input), input.getType()));
        }

        int group = equivalenceGroups.size();
        equivalenceGroups.put(group, new EquivalenceGroup(group, ImmutableList.of(expression)));

        return group;
    }

    public int getRootGroup()
    {
        return rootGroup;
    }
}

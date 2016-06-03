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
package com.facebook.presto.sql.optimizer.engine;

import com.facebook.presto.sql.optimizer.tree.Expression;

import java.util.HashMap;
import java.util.Map;

public class Memo2
{
    private final Map<String, Group> groupsById = new HashMap<>();
    private final Map<Expression, String> expressionMembership = new HashMap<>();
    private final Map<String, Expression> incomingReferences = new HashMap<>();

//    public String insert(Expression expression)
//    {
//        Expression rewritten = expression;
//        if (!expression.getArguments().isEmpty()) {
//            List<Group> children = expression.getArguments().stream()
//                    .map(argument -> insertInternal(argument))
//                    .collect(Collectors.toList());
//
//            List<Expression> arguments = children.stream()
//                    .map(Group::getId)
//                    .map(Reference::new)
//                    .collect(Collectors.toList());
//
//            rewritten = expression.copyWithArguments(arguments);
//
//            for (Group child : children) {
//                child.addReferrer(rewritten);
//            }
//        }
//
//        String name = expressionToGroup.get(rewritten);
//        Group group = groups.get(name);
//        if (name == null) {
//            group = createNewGroup();
//            expressionToGroup.put(rewritten, group.getId());
//        }
//
//        group.add(rewritten);
//
//        return group;
//    }
}

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
package com.facebook.presto.sql.optimizer.tree.type;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

public class RowTypeStamp
        extends TypeStamp
{
    private final List<TypeStamp> columnTypes;

    public RowTypeStamp(List<TypeStamp> columnTypes)
    {
        this.columnTypes = ImmutableList.copyOf(columnTypes);
    }

    public List<TypeStamp> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public String toString()
    {
        return "row(" + columnTypes.stream()
                .map(TypeStamp::toString)
                .collect(Collectors.joining(", ")) + ")";
    }
}

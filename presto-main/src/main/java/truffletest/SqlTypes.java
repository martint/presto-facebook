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
package truffletest;

import com.oracle.truffle.api.dsl.ImplicitCast;
import com.oracle.truffle.api.dsl.TypeCast;
import com.oracle.truffle.api.dsl.TypeCheck;
import com.oracle.truffle.api.dsl.TypeSystem;

@TypeSystem({int.class, NullableInt.class})
public class SqlTypes
{
    @ImplicitCast
    public NullableInt castNullableInt(int value)
    {
        return NullableInt.valueOf(value);
    }

    @TypeCast
    public int asInteger(Object value)
    {
        return (int) value;
    }

    @TypeCast
    public NullableInt asNullableInt(Object value)
    {
        if (isInteger(value)) {
            return NullableInt.valueOf((Integer) value);
        }

        return (NullableInt) value;
    }

    @TypeCheck
    public boolean isInteger(Object value)
    {
        return value instanceof Integer;
    }

    @TypeCheck
    public boolean isNullableInt(Object value)
    {
        return value instanceof NullableInt || isInteger(value);
    }

}

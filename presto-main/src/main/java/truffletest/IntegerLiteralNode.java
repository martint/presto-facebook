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

import com.oracle.truffle.api.dsl.Specialization;

public abstract class IntegerLiteralNode
    extends ExpressionNode
{
    private final int value;

    public IntegerLiteralNode(int value)
    {
        this.value = value;
    }

    @Specialization
    public int getValue()
    {
        return value;
    }

//    @Specialization
//    public NullableInt getNullableValue()
//    {
//        return NullableInt.valueOf(value);
//    }
}

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
package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.dsl.Specialization;

public abstract class LiteralNode
        extends ExpressionNode
{
    public abstract static class LongLiteral
            extends LiteralNode
    {
        private final long value;

        public LongLiteral(long value)
        {
            this.value = value;
        }

        @Specialization
        public long getValue()
        {
            return value;
        }
    }

    public abstract static class DoubleLiteral
            extends LiteralNode
    {
        private final double value;

        public DoubleLiteral(double value)
        {
            this.value = value;
        }

        @Specialization
        public double getValue()
        {
            return value;
        }
    }

    public abstract static class UnknownLiteral
            extends LiteralNode
    {
        @Specialization
        public Object getValue()
        {
            return null;
        }
    }
}

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

public class NullableInt
{
    private final int value;
    private boolean isNull;

    public NullableInt(int value, boolean isNull)
    {
        this.value = value;
        this.isNull = isNull;
    }

    public int getValue()
    {
        return value;
    }

    public boolean isNull()
    {
        return isNull;
    }

    public static NullableInt nullValue()
    {
        return new NullableInt(0, true);
    }

    public static NullableInt valueOf(int value)
    {
        return new NullableInt(value, false);
    }
}

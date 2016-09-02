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
package com.facebook.presto.sql.optimizer.engine2;

public class VersionedItem<T>
{
    private final T item;
    private final long version;

    public VersionedItem(T item, long version)
    {
        this.item = item;
        this.version = version;
    }

    public T get()
    {
        return item;
    }

    public long getVersion()
    {
        return version;
    }

    @Override
    public String toString()
    {
        return item + " @" + version;
    }
}

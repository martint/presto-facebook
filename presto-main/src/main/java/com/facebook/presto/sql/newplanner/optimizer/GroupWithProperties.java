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

final class GroupWithProperties
{
    private final int cluster;
    private final PhysicalConstraints requirements;

    public GroupWithProperties(int cluster, PhysicalConstraints requirements)
    {
        this.requirements = requirements;
        this.cluster = cluster;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GroupWithProperties that = (GroupWithProperties) o;

        if (cluster != that.cluster) {
            return false;
        }
        if (!requirements.equals(that.requirements)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = cluster;
        result = 31 * result + requirements.hashCode();
        return result;
    }
}

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
package com.facebook.presto.connector.thrift.server;

import com.facebook.swift.codec.guice.ThriftCodecModule;
import com.facebook.swift.service.guice.ThriftClientModule;
import com.facebook.swift.service.guice.ThriftServerModule;
import com.facebook.swift.service.guice.ThriftServerStatsModule;
import com.facebook.swift.service.guice.ThriftServiceExporter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class ThriftTpchServer
{
    private ThriftTpchServer()
    {
    }

    public static void start(List<Module> extraModules)
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new ThriftCodecModule())
                        .add(new ThriftClientModule())
                        .add(new ThriftServerModule())
                        .add(new ThriftServerStatsModule())
                        .add(binder -> {
                            binder.bind(ThriftIndexedTpchService.class).in(Scopes.SINGLETON);
                            ThriftServiceExporter.thriftServerBinder(binder).exportThriftService(ThriftIndexedTpchService.class);
                        })
                        .addAll(requireNonNull(extraModules, "extraModules is null"))
                        .build());
        app.strictConfig().initialize();
    }

    public static void main(String[] args)
    {
        try {
            ThriftTpchServer.start(ImmutableList.of());
        }
        catch (Throwable t) {
            Logger.get(ThriftTpchServer.class).error(t);
            System.exit(1);
        }
    }
}

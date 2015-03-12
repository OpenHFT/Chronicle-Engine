/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.client.internal;

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.map.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Rob Austin
 */
public class RemoteClientServiceLocator {

    private final ClientWiredStatelessTcpConnectionHub hub;
    private final ChronicleMap<String, ServiceDescriptor> serviceLocator;
    private final AtomicInteger nextFreeChannel = new AtomicInteger(2);

    public RemoteClientServiceLocator(@NotNull String hostname,
                                      int port,
                                      byte identifier) throws IOException {

        final ClientWiredChronicleMapStatelessBuilder<String, ServiceDescriptor> builder =
                new ClientWiredChronicleMapStatelessBuilder<>(
                        new InetSocketAddress(hostname, port),
                        String.class,
                        ServiceDescriptor.class,
                        (short) 1);
        builder.identifier(identifier);

        serviceLocator = builder.create();
        hub = builder.hub();
    }

    public ChronicleMap<String, ServiceDescriptor> serviceLocator() {
        return serviceLocator;
    }

    private <K, V> ChronicleMap<K, V> newMapInstance(@NotNull String name,
                                                    @NotNull Class<K> kClass,
                                                    @NotNull Class<V> vClass) throws IOException {
        final short channelID = findNextFreeChannel();
        final ServiceDescriptor serviceDescriptor = new ServiceDescriptor<K, V>(kClass, vClass, channelID);
        ((ChannelFactory) serviceLocator).createChannel(channelID);
        serviceLocator.put(name, serviceDescriptor);
        return mapInstance(kClass, vClass, channelID);
    }


    private <I, KI, VI> I mapInstance(Class<KI> kClass, Class<VI> vClass, short channelID) throws IOException {

        return (I) new ClientWiredChronicleMapStatelessBuilder<KI, VI>(
                hub,
                kClass,
                vClass,
                channelID).create();
    }

    public <I> I getService(Class<I> iClass, String name, Class... args) {

        try {
            final ChronicleMap<String, ServiceDescriptor> service = serviceLocator();
            final ServiceDescriptor<?, ?> serviceDescriptor = service.get(name);

            if (Map.class.isAssignableFrom(iClass)) {

                if (args.length < 2)
                    throw new IllegalStateException("please provide at least 2 args to create a Map");

                if (serviceDescriptor == null) {
                    final Class kClass = args[0];
                    final Class vClass = args[1];

                    return (I) newMapInstance(name, kClass, vClass);

                } else {
                    final Class<?> keyClass = serviceDescriptor.keyClass;
                    final Class<?> valueClass = serviceDescriptor.valueClass;
                    short channelID = serviceDescriptor.channelID;
                    return mapInstance(keyClass, valueClass, channelID);
                }

            }
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

        throw new IllegalStateException("iClass=" + iClass + " not supported");
    }


    // todo - this has to be improved so that is processor safe across all the node,
    // todo - just gonna increase an atomic integer for now, which is an extreemely dangerous assuption,
    // todo but this will get it to work for now  ( assuming you only ever have one client ) !
    private short findNextFreeChannel() {
        int c = nextFreeChannel.getAndIncrement();
        if (c > Short.MAX_VALUE)
            throw new IllegalStateException("too many channels allocated");
        return (short) c;
    }


}

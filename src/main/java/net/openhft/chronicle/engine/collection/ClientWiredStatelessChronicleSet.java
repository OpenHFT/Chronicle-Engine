package net.openhft.chronicle.engine.collection;

import net.openhft.chronicle.network.connection.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.wire.ValueIn;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class ClientWiredStatelessChronicleSet<U> extends
        ClientWiredStatelessChronicleCollection<U, Set<U>> implements Set<U> {

    public ClientWiredStatelessChronicleSet(@NotNull String channelName,
                                            @NotNull ClientWiredStatelessTcpConnectionHub hub,
                                            long cid,
                                            @NotNull Function<ValueIn, U> wireToSet,
                                            @NotNull String type) {
        super(channelName, hub, cid, wireToSet, type, HashSet::new);
    }
}
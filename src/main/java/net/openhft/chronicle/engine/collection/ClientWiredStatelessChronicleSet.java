package net.openhft.chronicle.engine.collection;

import net.openhft.chronicle.network.connection.TcpConnectionHub;
import net.openhft.chronicle.wire.ValueIn;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class ClientWiredStatelessChronicleSet<U> extends
        ClientWiredStatelessChronicleCollection<U, Set<U>> implements Set<U> {

    public ClientWiredStatelessChronicleSet(@NotNull TcpConnectionHub hub,
                                            @NotNull String csp, long cid,
                                            @NotNull Function<ValueIn, U> wireToSet) {
        super(hub, HashSet::new, wireToSet, csp, cid);
    }
}
package net.openhft.chronicle.map;

import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.wire.CoreFields;
import net.openhft.chronicle.wire.ParameterizeWireKey;
import net.openhft.chronicle.wire.ValueIn;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Created by Rob Austin
 */
public abstract class MapStatelessClient<E extends ParameterizeWireKey> extends AbstractStatelessClient<E> {

    /**
     * @param channelName
     * @param hub
     * @param cid         used by proxies such as the entry-set
     * @param csp
     */
    public MapStatelessClient(@NotNull String channelName,
                              @NotNull ClientWiredStatelessTcpConnectionHub hub,
                              long cid, String csp) {
        super(channelName, hub, cid, csp);

    }

    @Nullable
    protected <R> R proxyReturnTypedObject(
            @NotNull final E eventId,
            R usingValue,
            @NotNull final Class<R> resultType,
            @Nullable Object... args) {

        Function<ValueIn, R> consumerIn = resultType == CharSequence.class && usingValue != null
                ? f -> {
            f.textTo((StringBuilder) usingValue);
            return usingValue;
        }
                : f -> f.object(resultType);
        return proxyReturnWireConsumerInOut(eventId,
                CoreFields.reply,
                toParameters(eventId, args),
                consumerIn);
    }
}
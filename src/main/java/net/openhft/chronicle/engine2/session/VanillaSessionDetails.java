package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.SessionDetails;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by peter on 01/06/15.
 */
public class VanillaSessionDetails implements SessionDetails {
    private final Map<Class, Object> infoMap = new LinkedHashMap<>();
    private final String userId, securityToken;
    private final InetSocketAddress connectionAddress;
    private final long connectTimeMS;

    VanillaSessionDetails() {
        this.userId = null;
        this.securityToken = null;
        connectionAddress = null;
        connectTimeMS = System.currentTimeMillis();
    }

    public VanillaSessionDetails(String userId, String securityToken, InetSocketAddress connectionAddress, long connectTimeMS) {
        this.userId = userId;
        this.securityToken = securityToken;
        this.connectionAddress = connectionAddress;
        this.connectTimeMS = connectTimeMS;
    }

    @Override
    public String userId() {
        return userId;
    }

    @Override
    public String securityToken() {
        return securityToken;
    }

    @Override
    public InetSocketAddress connectionAddress() {
        return connectionAddress;
    }

    @Override
    public long connectTimeMS() {
        return connectTimeMS;
    }

    @Override
    public <I> void set(Class<I> infoClass, I info) {
        infoMap.put(infoClass, info);
    }

    @Override
    public <I> I get(Class<I> infoClass) {
        return (I) infoMap.get(infoClass);
    }
}

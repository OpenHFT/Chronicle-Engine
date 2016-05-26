package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.network.NetworkStats;

import java.util.UUID;

class WireNetworkStats implements NetworkStats {
    long writeBps, readBps, socketPollCountPerSecond;
    long timestamp;
    long localIdentifier;
    long remoteIdentifier;

    private int port;
    private UUID clientId;
    private String hostName;
    private String userId;

    public WireNetworkStats(int localIdentifer) {
        this.localIdentifier = localIdentifer;
    }

    @Override
    public WireNetworkStats hostName(String hostName) {
        this.hostName = hostName;
        return this;
    }

    @Override
    public String userId() {
        return userId;
    }

    @Override
    public WireNetworkStats userId(String userId) {
        this.userId = userId;
        return this;
    }

    @Override
    public long writeBps() {
        return writeBps;
    }

    @Override
    public WireNetworkStats writeBps(long writeBps) {
        this.writeBps = writeBps;
        return this;
    }

    @Override
    public long readBps() {
        return readBps;
    }

    @Override
    public WireNetworkStats readBps(long readBps) {
        this.readBps = readBps;
        return this;
    }

    @Override
    public long socketPollCountPerSecond() {
        return socketPollCountPerSecond;
    }

    @Override
    public WireNetworkStats socketPollCountPerSecond(long socketPollCountPerSecond) {
        this.socketPollCountPerSecond = socketPollCountPerSecond;
        return this;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public WireNetworkStats timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public void host(String hostName) {
        this.hostName = hostName;
    }

    @Override
    public void port(int port) {
        this.port = port;
    }

    @Override
    public long localIdentifier() {
        return localIdentifier;
    }

    @Override
    public WireNetworkStats localIdentifier(long localIdentifer) {
        this.localIdentifier = localIdentifer;
        return this;
    }

    @Override
    public long remoteIdentifier() {
        return remoteIdentifier;
    }

    @Override
    public WireNetworkStats remoteIdentifier(long remoteIdentifier) {
        this.remoteIdentifier = remoteIdentifier;
        return this;
    }

    @Override
    public void clientId(UUID clientId) {
        this.clientId = clientId;
    }


    @Override
    public UUID clientId() {
        return clientId;
    }

    @Override
    public String hostName() {
        return hostName;
    }

    @Override
    public int port() {
        return port;
    }
}
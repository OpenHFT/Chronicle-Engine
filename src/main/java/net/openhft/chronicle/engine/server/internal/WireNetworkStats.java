package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.network.NetworkStats;

import java.util.UUID;

class WireNetworkStats implements NetworkStats {
    long writeBps, readBps, socketPollCountPerSecond;
    long timestamp;
    long localIdentifier;
    long remoteIdentifier;
    String uuid;
    private UUID clientId;
    private String hostName;
    private int port;

    public WireNetworkStats(int localIdentifer) {
        this.localIdentifier = localIdentifer;
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

    public long localIdentifier() {
        return localIdentifier;
    }

    public WireNetworkStats localIdentifier(long localIdentifer) {
        this.localIdentifier = localIdentifer;
        return this;
    }

    public long remoteIdentifier() {
        return remoteIdentifier;
    }

    public WireNetworkStats remoteIdentifier(long remoteIdentifier) {
        this.remoteIdentifier = remoteIdentifier;
        return this;
    }

    public String uuid() {
        return uuid;
    }

    public WireNetworkStats uuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public void clientId(UUID clientId) {
        this.clientId = clientId;
    }


    public UUID clientId() {
        return clientId;
    }

    public String hostName() {
        return hostName;
    }

    public int port() {
        return port;
    }
}
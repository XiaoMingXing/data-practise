package com.nosql.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.net.InetSocketAddress;

public class CassandraConnector {

    private Cluster cluster;

    private Session session;

    public void connect(String node, Integer port) {
        InetSocketAddress address = InetSocketAddress.createUnresolved(node, port);
        Cluster.Builder point = Cluster.builder().addContactPointsWithPorts(address);
        cluster = point.build();
        session = cluster.connect();
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        session.close();
        cluster.close();
    }
}

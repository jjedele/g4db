package app_kvClient.performance;

import client.KVAdmin;
import client.KVAdminInterface;
import client.exceptions.ClientException;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.hash.Range;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Performance testing utilities: Enables a cluster for writing.
 * Arguments: node1:port ... nodeN:port
 */
public class EnableCluster {

    public static void main(String[] args) {
        List<InetSocketAddress> nodes = Stream.of(args)
                .map(s -> {
                    String[] parts = s.split(":");
                    return new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
                })
                .collect(Collectors.toList());

        Map<InetSocketAddress, KVAdminInterface> adminClients = new HashMap<>();
        nodes.forEach(node -> {
            KVAdminInterface admin = new KVAdmin(node);
            try {
                admin.connect();
            } catch (ClientException e) {
                e.printStackTrace();
            }
            adminClients.put(node, admin);
        });

        HashRing hashRing = new HashRing();

        nodes.forEach(hashRing::addNode);

        List<NodeEntry> nodeEntries = nodes.stream()
                .map(a -> new NodeEntry("nn", a, new Range()))
                .collect(Collectors.toList());

        adminClients.values().stream().forEach(activeConnection -> {
            try {
                activeConnection.updateMetadata(nodeEntries);
                activeConnection.start();
            } catch (ClientException e) {
                e.printStackTrace();
            }
        });

        adminClients.values().stream().forEach(activeConnection -> {
            activeConnection.disconnect();
        });
    }

}

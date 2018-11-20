package testing.performance;

import client.KVAdmin;
import client.KVAdminInterface;
import client.exceptions.ClientException;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.admin.MaintenanceStatusResponse;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Setup {

    public static void main(String[] args) throws ClientException, InterruptedException {
        InetSocketAddress node1 = new InetSocketAddress("localhost", 50000);
        KVAdminInterface s1admin = new KVAdmin(node1);
        s1admin.connect();
        InetSocketAddress node2 = new InetSocketAddress("localhost", 50001);
        KVAdminInterface s2admin = new KVAdmin(node2);
        s2admin.connect();

        HashRing hr1 = new HashRing();
        hr1.addNode(node1);

        HashRing hr2 = new HashRing();
        hr2.addNode(node1);
        hr2.addNode(node2);

        for (InetSocketAddress node : hr1.getNodes()) {
            System.out.println("before: " + node + " " + hr1.getAssignedRange(node));
        }

        for (InetSocketAddress node : hr2.getNodes()) {
            System.out.println("after: " + node + " " + hr2.getAssignedRange(node));
        }

        System.out.println("Transfer: " + hr2.getAssignedRange(node2) + " to " + node2);

        Collection<NodeEntry> nodes = Stream.of(node1, node2)
                .map(a -> new NodeEntry("sn", a, new Range()))
                .collect(Collectors.toList());

        s2admin.updateMetadata(nodes);

        s1admin.start();
        s2admin.start();

        s1admin.enableWriteLock();

        s1admin.moveData(node2, hr2.getAssignedRange(node2));

        MaintenanceStatusResponse statusResponse = s1admin.getMaintenanceStatus();
        while (statusResponse.isActive()) {
            System.out.println(statusResponse.getTask() + " " + statusResponse.getProgress());
            Thread.sleep(10000);
            statusResponse = s1admin.getMaintenanceStatus();
        }

        s1admin.disableWriteLock();

        s1admin.updateMetadata(nodes);

        s1admin.disconnect();
        s2admin.disconnect();
    }

}

package testing;

import common.hash.HashRing;
import common.hash.Range;
import junit.framework.TestCase;

import java.net.InetSocketAddress;

public class HashRingTest extends TestCase {

    static class TestHashRing extends HashRing {
        @Override
        protected int getHash(String val) {
            if (val.contains(":")) {
                // node -> hostname:port
                String[] parts = val.split(":");
                return Integer.parseInt(parts[1]);
            } else {
                // value -> we support only integers
                return Integer.parseInt(val);
            }
        }
    }

    public void testBasicResponsibility() {
        HashRing hashRing = new TestHashRing();

        InetSocketAddress node1 = new InetSocketAddress("localhost", 1);
        InetSocketAddress node2 = new InetSocketAddress("localhost", 5);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        InetSocketAddress responsibleNode = hashRing.getResponsibleNode("7");

        assertEquals(node1, responsibleNode);
    }

    public void testMD5Hash() {
        HashRing hashRing = new HashRing();

        int hash = hashRing.hash("hello world");

        System.out.println(hash);

        // does not throw exception
    }

    public void testGetSuccessor(){
        HashRing hashRing = new TestHashRing();

        InetSocketAddress node1 = new InetSocketAddress("localhost", 100);
        InetSocketAddress node2 = new InetSocketAddress("localhost", 5);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        InetSocketAddress getSuccessorNode = hashRing.getSuccessor(node1);

        assertEquals(getSuccessorNode, node2);

    }

    public void testGetNthSuccessor() {
        HashRing hashRing = new TestHashRing();

        InetSocketAddress node1 = new InetSocketAddress("localhost", 100);
        InetSocketAddress node2 = new InetSocketAddress("localhost", 5);
        InetSocketAddress node3 = new InetSocketAddress("localhost", 8);
        InetSocketAddress node4 = new InetSocketAddress("localhost", 11);
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);
        hashRing.addNode(node4);

        InetSocketAddress getNthSuccessorNode = hashRing.getNthSuccessor(node4, 2);

        assertEquals(getNthSuccessorNode, node2);
    }

    public void testGetAssignedRange(){
        HashRing hashRing = new TestHashRing();

        InetSocketAddress node1 = new InetSocketAddress("localhost", 1);
        InetSocketAddress node2 = new InetSocketAddress("localhost", 5);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        Range assignedRange = hashRing.getAssignedRange(node1);
        System.out.println(assignedRange);

    }

    public void testFindSuccessorNumber() {
        HashRing hashRing = new TestHashRing();

        InetSocketAddress node1 = new InetSocketAddress("localhost", 1);
        InetSocketAddress node2 = new InetSocketAddress("localhost", 5);
        InetSocketAddress node3 = new InetSocketAddress("localhost", 8);
        InetSocketAddress node4 = new InetSocketAddress("localhost", 11);
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);
        hashRing.addNode(node4);

        assertEquals(0, hashRing.findSuccessorNumber(node1, node1));
        assertEquals(3, hashRing.findSuccessorNumber(node1, node4));
        assertEquals(2, hashRing.findSuccessorNumber(node3, node1));
    }


}

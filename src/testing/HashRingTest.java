package testing;

import common.hash.HashRing;
import common.hash.Range;
import common.utils.HostAndPort;
import junit.framework.TestCase;

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

        HostAndPort node1 = new HostAndPort("localhost", 1);
        HostAndPort node2 = new HostAndPort("localhost", 5);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        HostAndPort responsibleNode = hashRing.getResponsibleNode("7");

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

        HostAndPort node1 = new HostAndPort("localhost", 100);
        HostAndPort node2 = new HostAndPort("localhost", 5);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        HostAndPort getSuccessorNode = hashRing.getSuccessor(node1);

        assertEquals(getSuccessorNode, node2);

    }

    public void testGetNthSuccessor() {
        HashRing hashRing = new TestHashRing();

        HostAndPort node1 = new HostAndPort("localhost", 100);
        HostAndPort node2 = new HostAndPort("localhost", 5);
        HostAndPort node3 = new HostAndPort("localhost", 8);
        HostAndPort node4 = new HostAndPort("localhost", 11);
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);
        hashRing.addNode(node4);

        HostAndPort getNthSuccessorNode = hashRing.getNthSuccessor(node4, 2);

        assertEquals(getNthSuccessorNode, node2);
    }

    public void testGetAssignedRange(){
        HashRing hashRing = new TestHashRing();

        HostAndPort node1 = new HostAndPort("localhost", 1);
        HostAndPort node2 = new HostAndPort("localhost", 5);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        Range assignedRange = hashRing.getAssignedRange(node1);
        System.out.println(assignedRange);

    }

    public void testFindSuccessorNumber() {
        HashRing hashRing = new TestHashRing();

        HostAndPort node1 = new HostAndPort("localhost", 1);
        HostAndPort node2 = new HostAndPort("localhost", 5);
        HostAndPort node3 = new HostAndPort("localhost", 8);
        HostAndPort node4 = new HostAndPort("localhost", 11);
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);
        hashRing.addNode(node4);

        assertEquals(0, hashRing.findSuccessorNumber(node1, node1));
        assertEquals(3, hashRing.findSuccessorNumber(node1, node4));
        assertEquals(2, hashRing.findSuccessorNumber(node3, node1));
    }


}

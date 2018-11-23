package testing;

import common.hash.HashRing;
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

        InetSocketAddress responsibleNode = hashRing.getResponsibleNode("3");

        assertEquals(node2, responsibleNode);
    }

    public void testMD5Hash() {
        HashRing hashRing = new HashRing();

        int hash = hashRing.hash("hello world");

        System.out.println(hash);

        // does not throw exception
    }

    public void getResponsibleNode() {
        HashRing hashRing = new TestHashRing();

        InetSocketAddress node1 = new InetSocketAddress("localhost", 1);
        InetSocketAddress node2 = new InetSocketAddress("localhost", 7);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        InetSocketAddress  ResponsibleNode = hashRing.getResponsibleNode("5");
        System.out.println(ResponsibleNode);

    }
    public void getSuccessor(){

    }

}

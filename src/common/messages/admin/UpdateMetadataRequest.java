package common.messages.admin;

import common.hash.NodeEntry;

import java.util.Collection;
import java.util.Collections;

/**
 * Requests the server to update it's metadata table to the one
 * provided in this message.
 */
public class UpdateMetadataRequest {

    /**
     * Add a node to the message.
     * @param node The node
     */
    public void addNode(NodeEntry node) {
        // TODO
    }

    /**
     * Return all nodes contained in this message.
     * @return Collection of nodes
     */
    public Collection<NodeEntry> getNodes() {
        // TODO
        return Collections.EMPTY_LIST;
    }

}

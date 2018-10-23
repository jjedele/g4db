package client;


import common.messages.KVMessage;

public class KVStore implements KVCommInterface {


    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void connect() throws Exception {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnect() {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KVMessage put(String key, String value) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KVMessage get(String key) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }


}

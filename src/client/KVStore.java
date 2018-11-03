package client;

import common.Protocol;
import common.exceptions.ProtocolException;
import common.exceptions.RemoteException;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.utils.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class KVStore implements KVCommInterface {

    private static final Logger LOG = LogManager.getLogger(KVStore.class);
    private static final byte RECORD_SEPARATOR = 0x1e;

    private final String address;
    private final int port;

    private Socket socket;
    private InputStream inputStream;
    private RecordReader inputReader;
    private OutputStream outputStream;
    private boolean connected;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.address = address;
        this.port = port;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void connect() throws IOException {
        socket = new Socket(address, port);
        inputStream = socket.getInputStream();
        inputReader = new RecordReader(inputStream, RECORD_SEPARATOR);
        outputStream = socket.getOutputStream();
        connected = true;
        LOG.info("Connected successfully to {} {}", address, port);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnect() {
        try {
            connected = false;
            outputStream.close();
            inputStream.close();
            socket.close();
        } catch (IOException e) {
            LOG.error("Error closing connection.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KVMessage put(String key, String value) throws IOException {
        // not my preferred API, but we want to match the interface

        if (value == null || "null".equals(value)) {
            return delete(key);
        }

        KVMessage outgoing = new DefaultKVMessage(key, value, KVMessage.StatusType.PUT);
        byte[] outgoingPayload = Protocol.encode(outgoing);
        send(outgoingPayload);

        byte[] replyPayload = receive();
        KVMessage reply = Protocol.decode(replyPayload);

        checkForError(reply);

        return reply;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KVMessage get(String key) throws IOException {
        KVMessage outgoing = new DefaultKVMessage(key, null, KVMessage.StatusType.GET);
        byte[] outgoingPayload = Protocol.encode(outgoing);
        send(outgoingPayload);

        byte[] replyPayload = receive();
        KVMessage reply = Protocol.decode(replyPayload);

        checkForError(reply);

        return reply;
    }

    /**
     * {@inheritDoc}
     */
    public KVMessage delete(String key) throws IOException {
        KVMessage outgoing = new DefaultKVMessage(key, null, KVMessage.StatusType.DELETE);
        byte[] outgoingPayload = Protocol.encode(outgoing);
        send(outgoingPayload);

        byte[] replyPayload = receive();
        KVMessage reply = Protocol.decode(replyPayload);

        checkForError(reply);

        return reply;
    }

    private void checkForError(KVMessage msg) throws ProtocolException {
        if (msg.getStatus() == KVMessage.StatusType.PUT_ERROR) {
            throw new RemoteException(
                    "Could not PUT value, server replied: "
                            + msg.getValue());
        } else if (msg.getStatus() == KVMessage.StatusType.GET_ERROR) {
            throw new RemoteException(
                    "Could not GET value, server replied: "
                            + msg.getValue());
        } else if (msg.getStatus() == KVMessage.StatusType.DELETE_ERROR) {
            throw new RemoteException(
                    "Could not DELETE value, server replied: "
                            + msg.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConnected() {
        return connected;
    }

    private void send(byte[] data) throws IOException {
        synchronized (outputStream) {
            outputStream.write(data);
            outputStream.write(RECORD_SEPARATOR);
        }
    }

    private byte[] receive() throws IOException {
        synchronized (inputStream) {
            return inputReader.read();
        }
    }

}

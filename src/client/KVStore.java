package client;

import client.exceptions.*;
import common.Protocol;
import common.exceptions.ProtocolException;
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
        this.connected = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void connect() throws ClientException {
        try {
            socket = new Socket(address, port);
            inputStream = socket.getInputStream();
            inputReader = new RecordReader(inputStream, RECORD_SEPARATOR);
            outputStream = socket.getOutputStream();
            connected = true;
            LOG.info("Connected successfully to {} {}", address, port);
        } catch (IOException e) {
            cleanConnectionShutdown();
            throw new ConnectionException(address, port);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnect() {
        cleanConnectionShutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KVMessage put(String key, String value) throws ClientException {
        // not my preferred API, but we want to match the interface
        if (value == null || "null".equals(value)) {
            return delete(key);
        }

        ensureConnected();

        KVMessage outgoing = new DefaultKVMessage(key, value, KVMessage.StatusType.PUT);
        byte[] outgoingPayload = Protocol.encode(outgoing);
        send(outgoingPayload);

        byte[] replyPayload = receive();
        KVMessage reply = parseResponse(replyPayload);

        return reply;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KVMessage get(String key) throws ClientException {
        ensureConnected();

        KVMessage outgoing = new DefaultKVMessage(key, null, KVMessage.StatusType.GET);
        byte[] outgoingPayload = Protocol.encode(outgoing);
        send(outgoingPayload);

        byte[] replyPayload = receive();
        KVMessage reply = parseResponse(replyPayload);

        return reply;
    }

    /**
     * {@inheritDoc}
     */
    public KVMessage delete(String key) throws ClientException {
        ensureConnected();

        KVMessage outgoing = new DefaultKVMessage(key, null, KVMessage.StatusType.DELETE);
        byte[] outgoingPayload = Protocol.encode(outgoing);
        send(outgoingPayload);

        byte[] replyPayload = receive();
        KVMessage reply = parseResponse(replyPayload);

        return reply;
    }

    private KVMessage parseResponse(byte[] replyPayload) throws ClientException {
        KVMessage reply;
        try {
            reply = Protocol.decode(replyPayload);
        } catch (ProtocolException e) {
            throw new CommunicationException("Could not decode reply.", e);
        }

        if (reply.getStatus() == KVMessage.StatusType.PUT_ERROR) {
            throw new ServerSideException(reply.getValue());
        } else if (reply.getStatus() == KVMessage.StatusType.GET_ERROR) {
            throw new ServerSideException(reply.getValue());
        } else if (reply.getStatus() == KVMessage.StatusType.DELETE_ERROR) {
            throw new ServerSideException(reply.getValue());
        }

        return reply;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConnected() {
        return connected;
    }

    private void send(byte[] data) throws CommunicationException {
        try {
            synchronized (outputStream) {
                outputStream.write(data);
                outputStream.write(RECORD_SEPARATOR);
            }
        } catch (IOException e) {
            cleanConnectionShutdown();
            throw new CommunicationException("Could not send data.", e);
        }
    }

    private byte[] receive() throws CommunicationException {
        try {
            synchronized (inputStream) {
                return inputReader.read();
            }
        } catch (IOException e) {
            cleanConnectionShutdown();
            throw new CommunicationException("Could not receive data.", e);
        }
    }

    private void ensureConnected() throws ClientException {
        if (!connected) {
            throw new DisconnectedException();
        }
    }

    private void cleanConnectionShutdown() {
        LOG.info("Closing connection.");
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                LOG.error("Error closing connection.", e);
            }
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                LOG.error("Error closing connection.", e);
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.error("Error closing connection.", e);
            }
        }
        connected = false;
    }

}

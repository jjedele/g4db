package client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Default socket implementation of the database {@link Client}.
 * @deprecated Will be replaced by {@link KVStore}
 */
@Deprecated
public class SocketClient implements Client {

    private static final Logger LOG = LogManager.getLogger(SocketClient.class);

    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private boolean connected;

    /**
     * {@inheritDoc}
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String connect(String hostname, int port) throws IOException {
        socket = new Socket(hostname, port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        connected = true;
        return receiveString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnect() throws IOException {
        outputStream.close();
        inputStream.close();
        socket.close();
        connected = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String sendMessage(String message) throws IOException {
        sendString(message);
        String reply = receiveString();
        return reply;
     }

    /**
     * Send binary data to the server.
     * @param bytes the payload
     * @throws IOException if something network related goes wrong
     */
    public void send(byte[] bytes) throws IOException {
        outputStream.write(bytes);
        outputStream.write('\r');
        outputStream.flush();
    }

    /**
     * Receive binary data from the server.
     * @return the received binary data
     * @throws IOException if something network related goes wrong
     */
    public byte[] receive() throws IOException {
        byte[] buffer = new byte[8192];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int count;
        while ((count = inputStream.read(buffer)) > 0) {
            bos.write(buffer, 0, count);
            if (buffer[count - 1] == '\r') {
                break;
            }
        }
        return bos.toByteArray();
    }

    /**
     * Send string message to server.
     * @param message the message
     * @throws IOException if something network related goes wrong
     */
    public void sendString(String message) throws IOException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        send(bytes);
    }

    /**
     * Receive a string message from a server.
     * @return the message
     * @throws IOException if something network related goes wrong
     */
    public String receiveString() throws  IOException {
        byte[] bytes = receive();
        String string = new String(bytes, StandardCharsets.UTF_8);
        return string;
    }

}

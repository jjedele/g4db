package app_kvClient.commands;

import app_kvClient.KVClient;
import client.KVCommInterface;
import client.KVInterface;
import client.KVStore;
import client.exceptions.ClientException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * The ConnectCommand is responsible for establishing a connection between
 * client and server.
 */
public class ConnectCommand implements Command {

    /** ID of this command. */
    public static final String ID = "connect";

    private String host;
    private int port;

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public List<Argument> getArguments() {
        return Arrays.asList(
            new Argument("host", "Host name of the server"),
            new Argument("port", "Port to connect to")
        );
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Connect to the specified server.";
    }

    /** {@inheritDoc} */
    @Override
    public void init(String[] args) throws CommandException {
        // host name
        if (args.length != 2) {
            throw new CommandException("Wrong number of arguments", this);
        }

        this.host = args[0];

        // port number
        try {
            this.port = Integer.parseUnsignedInt(args[1]);
        } catch (NumberFormatException e) {
            throw new CommandException("Port not parsable", this);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String run(KVClient cli) throws CommandException {
        if (cli.getClient().map(KVInterface::isConnected).orElse(false)) {
            throw new CommandException("Already connected, please disconnect first.", this);
        }

        try {
            KVCommInterface client = new KVStore(host, port);
            cli.setClient(client);
            client.connect();
        } catch (ClientException e) {
            throw new CommandException(e.getMessage(), this, e);
        }
        return String.format("Successfully connected to %s:%d", host, port);
    }

}

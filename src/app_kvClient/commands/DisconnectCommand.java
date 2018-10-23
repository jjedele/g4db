package app_kvClient.commands;

import app_kvClient.KVClient;
import client.Client;

import java.io.IOException;

public class DisconnectCommand implements Command {

    /**
     * ID of this command.
     */
    public static final String ID = "disconnect";

    /**
     * {@inheritDoc}
     */
    @Override
    public String getID() {
        return ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "Disconnect the application.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String run(KVClient cli) throws CommandException {
        Client client = cli.getClient();

        if (!client.isConnected()) {
            throw new CommandException("Not connected.", this);
        }

        try {
            client.disconnect();
        } catch (IOException e) {
            throw new CommandException("Could not connect socket.", this, e);
        }
        return "Disconnected";
    }
}

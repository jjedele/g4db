package app_kvClient.commands;

import app_kvClient.KVClient;

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
        if (!cli.getClient().isConnected()) {
            throw new CommandException("Not connected.", this);
        }

        cli.getClient().disconnect();
        return "Disconnected successfully.";
    }
}

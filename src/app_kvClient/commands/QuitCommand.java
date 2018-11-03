package app_kvClient.commands;

import app_kvClient.KVClient;

/**
 * The QuitCommand is responsible for shutting down the KVClient.
 */
public class QuitCommand implements Command {

    /** ID of this command. */
    public static final String ID = "quit";

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Quit the application.";
    }

    /** {@inheritDoc} */
    @Override
    public String run(KVClient cli) {
        cli.getClient().disconnect();
        cli.setExiting(true);
        return "Shutting down now - Good bye!";
    }

}

package app_kvClient.commands;

import app_kvClient.KVClient;
import client.Client;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SendCommand implements Command {

    /** ID of this command. */
    public static final String ID = "send";

    private String message;

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public List<Command.Argument> getArguments() {
        return Arrays.asList(
                new Command.Argument("message", "Message to send. Must be quoted if it contains spaces.")
        );
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Send message to server and get reply";
    }

    /** {@inheritDoc} */
    @Override
    public void init(String[] args) throws CommandException {
        // host name
        if (args.length != 1) {
            throw new CommandException("Wrong number of arguments", this);
        }
        this.message = args[0];
    }

    /** {@inheritDoc} */
    @Override
    public String run(KVClient cli) throws CommandException {
        Client client = cli.getClient();

        if (!client.isConnected()) {
            throw new CommandException("Not connected.", this);
        }

        String reply = null;
        try {
            reply = client.sendMessage(message);
        } catch (IOException e) {
            throw new CommandException("Could not send message.", this, e);
        }
        return reply;
    }

}

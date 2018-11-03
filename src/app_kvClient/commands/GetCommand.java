package app_kvClient.commands;

import app_kvClient.KVClient;
import client.KVCommInterface;
import common.exceptions.RemoteException;
import common.messages.KVMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * The GetCommand represents a request to retrieve a stored
 * value from the server.
 */
public class GetCommand implements Command {

    /** ID of this command. */
    public static final String ID = "get";

    private String key;

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public List<Argument> getArguments() {
        return Arrays.asList(
                new Argument("key", "Unique key of the information.")
        );
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Get key and its value from the server";
    }

    /** {@inheritDoc} */
    @Override
    public void init(String[] args) throws CommandException {
        // host name
        if (args.length != 1) {
            throw new CommandException("Wrong number of arguments", this);
        }
        this.key = args[0];
    }

    /** {@inheritDoc} */
    @Override
    public String run(KVClient cli) throws CommandException {
        KVCommInterface client = cli.getClient();

        if (client == null) {
            throw new CommandException("Not connected.", this);
        }

        String reply = null;
        try {
            KVMessage serverReply = client.get(key);
            reply = serverReply.toString();
        } catch (RemoteException e) {
            throw new CommandException(e.getMessage(), this, e);
        } catch (IOException e) {
            throw new CommandException("Could not get message.", this, e);
        }
        return reply;
    }

}

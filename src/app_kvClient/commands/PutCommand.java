package app_kvClient.commands;

import app_kvClient.KVClient;
import client.KVCommInterface;
import client.exceptions.ClientException;
import common.exceptions.RemoteException;
import common.messages.KVMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * The PutCommand represents a request to store a value for a key
 * on the server.
 */
public class PutCommand implements Command {

    /** ID of this command. */
    public static final String ID = "put";

    private String key;
    private String value;

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public List<Argument> getArguments() {
        return Arrays.asList(
                new Argument("key", "Key of the data. " +
                        "Enclose in quotes to include whitespace."),
                new Argument("value", "Value of the data. " +
                        "Enclose in quotes to include whitespace. " +
                        "Use \"null\" to delete the data.")
        );
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Store value to the server's storage";
    }

    /** {@inheritDoc} */
    @Override
    public void init(String[] args) throws CommandException {
        // host name
        if (args.length != 2) {
            throw new CommandException("Wrong number of arguments", this);
        }
        this.key = args[0];
        this.value = args[1];
    }

    /** {@inheritDoc} */
    @Override
    public String run(KVClient cli) throws CommandException {
        KVCommInterface client = cli.getClient();

        String reply = null;
        try {
            KVMessage serverReply = client.put(key,value);
            reply = String.format("%s %s : %s", serverReply.getStatus().name(),
                    serverReply.getKey(), serverReply.getValue());
        } catch (ClientException e) {
            throw new CommandException(e.getMessage(), this, e);
        }
        return reply;
    }

}

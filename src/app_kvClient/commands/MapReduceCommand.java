package app_kvClient.commands;

import app_kvClient.KVClient;
import client.KVStore;
import client.exceptions.ClientException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * The MapReduce command initiates a map/reduce aggregation instance
 * on the server.
 */
public class MapReduceCommand implements Command {

    /** ID of this command. */
    public static final String ID = "mapReduce";

    private String targetNamespace;
    private String scriptPath;

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public List<Argument> getArguments() {
        return Arrays.asList(
                new Argument("targetNamespace", "Namespace the results will be written to."),
                new Argument("scriptPath", "Path to the map/reduce script," +
                        "relative to current working directory.")
        );
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Initiate a new map/reduce aggregation.";
    }

    /** {@inheritDoc} */
    @Override
    public void init(String[] args) throws CommandException {
        if (args.length != 2) {
            throw new CommandException("Wrong number of arguments", this);
        }

        this.targetNamespace = args[0];
        this.scriptPath = args[1];
    }

    /** {@inheritDoc} */
    @Override
    public String run(KVClient cli) throws CommandException {
        KVStore client = (KVStore) cli
                .getClient()
                .orElseThrow(() -> new CommandException("Not connected.", this));

        String script = null;
        try {
            script = new String(Files.readAllBytes(Paths.get(scriptPath)));
        } catch (IOException e) {
            throw new CommandException("Cannot read script file " + scriptPath, this, e);
        }

        try {
            String id = client.mapReduce(targetNamespace, script);
            return String.format("Started map/reduce job with id: " + id);
        } catch (ClientException e) {
            throw new CommandException(e.getMessage(), this, e);
        }
    }

}

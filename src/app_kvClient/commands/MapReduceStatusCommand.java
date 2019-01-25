package app_kvClient.commands;

import app_kvClient.KVClient;
import client.KVStore;
import client.exceptions.ClientException;

import java.util.Arrays;
import java.util.List;

public class MapReduceStatusCommand implements Command {

    public static final String ID = "mapReduceStatus";

    private String jobId;

    /**
     * Get the unique identifier by which the command is called in the KVClient.
     *
     * @return ID
     */
    @Override
    public String getID() {
        return ID;
    }

    @Override
    public List<Argument> getArguments() {
        return Arrays.asList(
                new Argument("jobID", "mapReduce job ID")
        );
    }

    /**
     * Get a description of this command.
     *
     * @return The description
     */
    @Override
    public String getDescription() {
        return "Get the job status";
    }

    @Override
    public void init(String[] args) throws CommandException {
        if(args.length == 1) {
            this.jobId = args[0];
        } else {
            throw new CommandException("Wrong number of arguments", this);
        }
    }

    /**
     * Execute this command.
     *
     * @param cli The KVClient instance to operate upon
     * @return Result of the command in String format
     * @throws CommandException If there is a problem executing the command
     */
    @Override
    public String run(KVClient cli) throws CommandException {
        KVStore client = (KVStore) cli
                .getClient()
                .orElseThrow(() -> new CommandException("Not connected", this));

        try {
            String status = client.getMapReduceStatus(jobId).toString();
            return "Job Status for id: " + status;
        } catch (ClientException e) {
            throw new CommandException(e.getMessage(), this, e);
        }
    }
}

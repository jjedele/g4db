package app_kvClient.commands;

import app_kvClient.KVClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class HelpCommand implements Command {

    /** ID of this command. */
    public static final String ID = "help";

    private static final Logger LOG = LogManager.getLogger(HelpCommand.class);

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public List<Argument> getArguments() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Describe all available commands.";
    }

    /** {@inheritDoc} */
    @Override
    public void init(String[] args) {
        // do nothing
    }

    /** {@inheritDoc} */
    @Override
    public String run(KVClient cli) {
        StringBuilder help = new StringBuilder("Available commands:\n\n");
        for (Class<? extends Command> commandClass : cli.getCommands().values()) {
            try {
                Command command = commandClass.newInstance();
                help.append(cli.getUsageInformation(command));
                help.append('\n');
            } catch (InstantiationException | IllegalAccessException e) {
                LOG.error("Could not instantiate command.", e);
            }
        }
        return help.toString();
    }
}

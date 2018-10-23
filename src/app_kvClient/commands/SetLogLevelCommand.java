package app_kvClient.commands;

import app_kvClient.KVClient;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Arrays;
import java.util.List;

/**
 * TheSetLogLevelCommand is responsible for  change log level
 */
public class SetLogLevelCommand implements Command {

    /** ID of this command. */
    public static final String ID = "logLevel";

    private Level level;

    /** {@inheritDoc} */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public List<Argument> getArguments() {
        StringBuilder builder = new StringBuilder("Available: ");
        for (Level level : Level.values()) {
            builder.append(level.name());
            builder.append(' ');
        }
        return Arrays.asList(
            new Argument("level", "Log level of the application. " + builder.toString())

        );
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Change log level to specified level.";
    }

    /** {@inheritDoc} */
    @Override
    public void init(String[] args) throws CommandException {
        // level name
        if (args.length != 1) {
            throw new CommandException("Wrong number of arguments", this);
        }

        this.level = Level.getLevel(args[0]);
        if (this.level == null){
            throw new CommandException("Wrong level", this);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String run(KVClient cli) {
        Configurator.setAllLevels(LogManager.getRootLogger().getName(), level);
        return "Changed level to " + level;
    }

}

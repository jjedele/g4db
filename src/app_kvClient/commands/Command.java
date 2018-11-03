package app_kvClient.commands;

import app_kvClient.KVClient;
import java.util.Collections;
import java.util.List;

/**
 * Base interface for a KVClient command.
 */
public interface Command {

    /**
     * Representation of an argument for a command.
     */
    class Argument {

        /**
         * Name of the argument.
         */
        public final String name;

        /**
         * Description of the argument.
         */
        public final String description;

        /**
         * Default constructor.
         * @param name Name of the argument
         * @param description Description of the argument
         */
        public Argument(final String name, final String description) {
            this.name = name;
            this.description = description;
        }

    }

    /**
     * Get the unique identifier by which the command is called in the KVClient.
     * @return ID
     */
    String getID();

    /**
     * Get a list of all arguments necessary for this command.
     * @return List of arguments
     */
    default List<Argument> getArguments() {
        return Collections.emptyList();
    }

    /**
     * Get a description of this command.
     * @return The description
     */
    String getDescription();

    /**
     * Initialize this command by parsing the arguments.
     *
     * This happens before the command is executed by contract.
     * @param args Arguments for the command in string format
     * @throws CommandException If there is a problem parsing the arguments
     */
    default void init(String[] args) throws CommandException {
        // do nothing
    }

    /**
     * Execute this command.
     *
     * @param cli The KVClient instance to operate upon
     * @return Result of the command in String format
     * @throws CommandException If there is a problem executing the command
     */
    String run(KVClient cli) throws CommandException;


}

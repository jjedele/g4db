package app_kvClient.commands;

/**
 * Base exception that should be used to encode all problems related
 * to command initialization and execution.
 *
 * It will be displayed in the UI with a concise error message and a
 * summary of how to use the command which produced the error.
 */
public class CommandException extends Exception {

    private final Command command;

    /**
     * Default constructor.
     * @param message Concise description of the problem.
     * @param command Command that is source of the problem.
     */
    public CommandException(String message, Command command) {
        super(message);
        this.command = command;
    }

    /**
     * Default constructor.
     * @param message Concise description of the problem.
     * @param command Command that is source of the problem.
     * @param cause The exception that caused this one.
     */
    public CommandException(String message, Command command, Exception cause) {
        super(message, cause);
        this.command = command;
    }

    /**
     * Get the command the is source of the problem.
     * @return The command
     */
    public Command getCommand() {
        return command;
    }

}

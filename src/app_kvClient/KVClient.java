package app_kvClient;

import app_kvClient.commands.*;
import client.KVCommInterface;
import client.KVStore;
import client.exceptions.ServerSideException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The interactive command line interface (KVClient) for the client.
 */
public class KVClient {

    private static final Logger LOG = LogManager.getLogger(KVClient.class);

    private KVCommInterface client;
    private boolean exiting;
    private final Map<String, Class<? extends Command>> commands;

    /**
     * Run the main read-eval-print-loop (REPL).
     * @param args Provided command line arguments
     * @throws IOException If stdin is not readable or stdout is not writable
     */
    public static void main(String[] args) throws IOException {
        KVClient cli = new KVClient();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        // REPL
        while (!cli.isExiting()) {
            // prompt
            System.out.print(cli.getPrompt());

            // input
            String input = reader.readLine();

            if (input != null && input.length() > 0) {
                try {
                    // try to parse the command
                    Optional<Command> parseResult = cli.parseInput(input);

                    // if command not known, print error and default to the help command
                    if (!parseResult.isPresent()) {
                        System.out.println(String.format("ERROR: No such command: '%s'", input));

                        Command helpCommand = new HelpCommand();
                        helpCommand.init(new String[] {});
                        parseResult = Optional.of(helpCommand);
                    }

                    // execute and print output
                    String result = parseResult.get().run(cli);
                    System.out.println(result);
                } catch (CommandException e) {
                    System.out.println(cli.formatException(e));
                    LOG.error("Could not execute command.", e);
                } catch (Exception e) {
                    System.err.println("Unexpected error: " + e);
                    LOG.error("Unexpected error while executing command.", e);
                }
            }
        }
    }

    /**
     * Default constructor.
     */
    public KVClient() {
        this.exiting = false;
        this.client = new KVStore("localhost", 50000);

        // TODO: at some point we could use reflection for this
        this.commands = new HashMap<>();
        this.commands.put(QuitCommand.ID, QuitCommand.class);
        this.commands.put(ConnectCommand.ID, ConnectCommand.class);
        this.commands.put(DisconnectCommand.ID, DisconnectCommand.class);
        this.commands.put(HelpCommand.ID, HelpCommand.class);
        this.commands.put(PutCommand.ID, PutCommand.class);
        this.commands.put(GetCommand.ID, GetCommand.class);
        this.commands.put(SetLogLevelCommand.ID, SetLogLevelCommand.class);
    }

    /**
     * Parse an user input to an instance of a {@link Command}.
     * @param input The input of the user
     * @return An {@link Optional} holding an instance of a runnable {@link Command} or
     *         an empty {@link Optional} if no command corresponding to the input could be found.
     * @throws CommandException If there was a problem initializing the command with provided arguments.
     */
    public Optional<Command> parseInput(final String input) throws CommandException {
        // TODO I don't like that this can both return an empty optional and throw an exception
        String[] parts = splitArgumentString(input);
        String name = parts[0];
        String[] arguments = Arrays.copyOfRange(parts, 1, parts.length);

        if (!this.commands.containsKey(name)) {
            // no command known for given input
            return Optional.empty();
        }

        Class<? extends Command> commandClass = this.commands.get(name);
        try {
            Command command = commandClass.newInstance();
            LOG.debug("Initializing command {} with arguments {}",
                    command.getClass(),
                    Arrays.toString(arguments));

            command.init(arguments);

            LOG.debug("Initialized command {}", command);

            return Optional.of(command);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Could not instantiate command.", e);
        }
    }

    /**
     * Compose a readable form of a {@link CommandException} for the UI.
     * @return The exception summary
     */
    public String formatException(CommandException e) {
        StringBuilder builder = new StringBuilder();
        if (e.getCause() instanceof ServerSideException) {
            builder.append("SERVER ERROR: ");
        } else {
            builder.append("ERROR: ");
        }
        builder.append(e.getMessage());
        builder.append('\n');
        builder.append(getUsageInformation(e.getCommand()));
        return builder.toString();
    }

    /**
     * Create a readable summary of how to use a command.
     * @return The summary
     */
    public String getUsageInformation(Command command) {
        StringBuilder info = new StringBuilder();

        // usage line
        info.append("Command: ");
        info.append(command.getID());
        for (Command.Argument argument : command.getArguments()) {
            info.append(" <");
            info.append(argument.name);
            info.append('>');
        }
        info.append('\n');

        // description line
        info.append(command.getDescription());
        info.append('\n');

        // details about arguments if available
        if (!command.getArguments().isEmpty()) {
            info.append("Arguments:\n");
            for (Command.Argument argument : command.getArguments()) {
                info.append("  ");
                info.append(argument.name);
                info.append(" : ");
                info.append(argument.description);
                info.append('\n');
            }
        }

        return info.toString();
    }

    /**
     * Get the {@link KVCommInterface} associated with this KVClient.
     * @return The client
     */
    public KVCommInterface getClient() {
        return client;
    }

    /**
     * Set the {@link KVCommInterface} associated with this KVClient.
     * @param client The client
     */
    public void setClient(KVCommInterface client) {
        this.client = client;
    }

    /**
     * Get the prompt string set for this KVClient.
     * @return The prompt string
     */
    public String getPrompt() {
        StringBuilder builder = new StringBuilder("EchoClient");
        if (client.isConnected()) {
            builder.append(" (connected)");
        }
        builder.append("> ");
        return builder.toString();
    }

    /**
     * Get the registered {@link Command}s for this KVClient.
     * @return Mapping of command ID to command class
     */
    public Map<String, Class<? extends Command>> getCommands() {
        return commands;
    }

    /**
     * Get the exiting state of this KVClient.
     * @return If the KVClient is scheduled to exit
     */
    public boolean isExiting() {
        return exiting;
    }

    /**
     * Set the exiting state of this KVClient.
     * @param exiting The exiting state
     */
    public void setExiting(boolean exiting) {
        this.exiting = exiting;
    }

    /**
     * Split a joint string of arguments into individual parts.
     *
     * This considers quoted strings. Example:
     * foo bar "baz boom" -> ["foo", "bar", "baz boom"]
     *
     * @param argString the joint argument string
     * @return the split elements
     */
    static String[] splitArgumentString(String argString) {
        List<String> args = new ArrayList<>();
        // this is inspired by aioobe
        // https://stackoverflow.com/questions/7804335/split-string-on-spaces-in-java-except-if-between-quotes-i-e-treat-hello-wor
        Pattern pattern = Pattern.compile("([^\"]\\S*|\".+?\")\\s*");
        Matcher matcher = pattern.matcher(argString);
        while (matcher.find()) {
            args.add(matcher.group(1).replaceAll("\"", ""));
        }
        return args.toArray(new String[] {});
    }

}

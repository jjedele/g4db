package client.exceptions;

public class DisconnectedException extends ClientException {

    private static final String MSG = "Not connected.";

    public DisconnectedException() {
        super(MSG);
    }

}

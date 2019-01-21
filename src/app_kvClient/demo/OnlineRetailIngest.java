package app_kvClient.demo;

import client.KVStore;
import client.exceptions.ClientException;
import common.messages.KVMessage;
import common.utils.HostAndPort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data ingestion tool for the online retail map/reduce demo.
 *
 * Arguments: HOST:PORT PATH_TO_DATA_FILE NUMBER_OF_RECORDS_TO_IMPORT
 */
public class OnlineRetailIngest {

    static HostAndPort parseAddress(String hostAndPort) {
        String[] parts = hostAndPort.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        return new HostAndPort(host, port);
    }

    static private final Pattern ID_PATTERN = Pattern.compile("\"invoice_no\": *\"(.+?)\"");
    static String parseInvoiceId(String record) {
        Matcher matcher = ID_PATTERN.matcher(record);
        matcher.find();
        return matcher.group(1);
    }

    public static void main(String[] args) throws IOException, ClientException {
        if (args.length < 2) {
            System.err.println("Call with: HOST:PORT PATH_TO_FILE NO_OF_ORDERS");
            System.exit(1);
        }

        String hostAndPort = args[0];
        String path = args[1];

        int numberOfOrders = 500000;
        if (args.length == 3) {
            numberOfOrders = Integer.parseInt(args[2]);
        }

        HostAndPort address = parseAddress(hostAndPort);
        Path dataFile = Paths.get(path);

        KVStore store = new KVStore(address.getHost(), address.getPort());
        store.connect();
        Files.lines(dataFile).limit(numberOfOrders).forEach(record -> {
            String key = parseInvoiceId(record);
            try {
                KVMessage reply = store.put(key, record);
                System.out.println(reply);
            } catch (ClientException e) {
                e.printStackTrace();
            }
        });
        store.disconnect();
    }

}

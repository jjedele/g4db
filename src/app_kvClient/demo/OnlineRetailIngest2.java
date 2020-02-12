package app_kvClient.demo;

import client.KVStore;
import client.exceptions.ClientException;
import common.messages.KVMessage;
import common.utils.HostAndPort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
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
public class OnlineRetailIngest2 {

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
        Path dataFile = Paths.get("demo_data", "OnlineRetail.json.txt");

        for (int i = 0; i < 15; i++) {
            final int j = i;
            Files.lines(dataFile).forEach(record -> {
                String key = "" + j + "_" + parseInvoiceId(record);
                File outFile = new File("../proc_data/" + key);
                try {
                    PrintWriter printWriter = new PrintWriter(outFile);
                    printWriter.println(record);
                    printWriter.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            });
        }
    }

}

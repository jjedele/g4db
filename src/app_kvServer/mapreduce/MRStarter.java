package app_kvServer.mapreduce;


import client.KVStore;
import client.exceptions.ClientException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/*
 * TODO remove again
 */
public class MRStarter {

    public static void main(String[] args) throws IOException, ClientException {
        Path scriptFilePath = Paths.get(System.getProperty("user.dir"), "demo_data", "sales_by_country.js");
        String script = new String(Files.readAllBytes(scriptFilePath));

        KVStore kvStore = new KVStore("localhost", 10000);
        kvStore.connect();

        kvStore.mapReduce("bycountry", script);

        kvStore.disconnect();
    }

}

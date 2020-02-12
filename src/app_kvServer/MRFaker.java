package app_kvServer;


import app_kvServer.mapreduce.MapReduceProcessor;
import client.KVStore;
import client.exceptions.ClientException;
import common.messages.KVMessage;
import common.utils.RecordReader;

import javax.script.ScriptException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/*
 * TODO remove again
 */
public class MRFaker {

    public static long process(int n) throws IOException, ScriptException {
        Path scriptFilePath = Paths.get(System.getProperty("user.dir"), "demo_data", "sales_by_country.js");
        String script = new String(Files.readAllBytes(scriptFilePath));

        Instant start = Instant.now();
        MapReduceProcessor processor = new MapReduceProcessor(script);

        File dataDir = new File("../proc_data");

        Arrays.stream(dataDir.listFiles()).limit(n).forEach(dataFile -> {
            try {
                FileInputStream fis = new FileInputStream(dataFile);
                RecordReader reader = new RecordReader(fis, (byte) '\n');
                String data = new String(reader.read());
                String key = "";
                processor.process(key, data);
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        Instant end = Instant.now();

        return Duration.between(start, end).getSeconds();
    }

    public static void main(String[] args) throws IOException, ScriptException {
        int[] batches = {39750, 397500, 198750, 132500, 99375, 79500, 66250, 56786, 49687, 44167};

        for (int batchSize : batches) {
            for (int i = 0; i < 10; i++) {
                long dur = process(batchSize);
                System.out.printf("%d;%d\n", batchSize, dur);
            }
        }
    }

}

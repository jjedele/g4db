package app_kvClient.performance;

import client.KVStore;
import client.exceptions.ClientException;
import common.messages.KVMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.function.IntSupplier;

/**
 * Performance testing utilities: Reads entries for keys simulating different scenarios.
 * Arguments: node1:port
 */
public class DistributionalDataReader {

    public static void main(String[] args) throws ClientException, FileNotFoundException {
        if (args.length < 1) {
            throw new RuntimeException("Must at least provide seed address.");
        }
        String[] parts = args[0].split(":");
        InetSocketAddress server = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));

        KVStore client = new KVStore(server.getHostString(), server.getPort());

        String distribution = "uniform";
        if (args.length >= 2) {
            distribution = args[1];
        }

        final int keyRange;
        if (args.length >= 3) {
            keyRange = Integer.parseInt(args[2]);
        } else {
            keyRange = 100000;
        }

        Random rand = new Random();

        int payloadSize = 1000;
        StringBuilder data = new StringBuilder();
        rand.ints(payloadSize, 'A', 'z')
                .forEach(c -> data.append((char) c));
        String strData = data.toString();

        // key generator function
        IntSupplier generateKey = () -> rand.nextInt(keyRange);
        if ("hotspot".equals(distribution)) {
            int hotSpotSize = keyRange / 100;
            generateKey = () -> {
                if (rand.nextFloat() <= 0.7) {
                    return rand.nextInt(hotSpotSize);
                } else {
                    return hotSpotSize + rand.nextInt(keyRange - hotSpotSize);
                }
            };
        } else if ("moving".equals("distribution")) {
            int hotSpotSize = keyRange / 100;
            int stableSeconds = 1;
            generateKey = () -> {
                int center = (int) (System.currentTimeMillis() / 1000 / stableSeconds) % (keyRange - hotSpotSize);
                center += hotSpotSize;
                int offset = rand.nextInt(hotSpotSize);
                return center - offset;
            };
        }


        // read
        PrintWriter pw = new PrintWriter(new File("data.csv"));
        int batchSize = 10;
        try {
            // ingest data for key range
            client.connect();
            for (int i = 0; i < keyRange; i++) {
                String key = "k" + i;
                client.put(key, strData);
                if (i % 1000 == 0) {
                    System.out.println("Ingested 1000 keys.");
                }
            }

            int count = 0;
            int epoch = 0;
            Instant start = Instant.now();
            while (true) {
                String key = "k" + generateKey.getAsInt();

                KVMessage reply = client.get(key);
                if (reply.getStatus() != KVMessage.StatusType.GET_SUCCESS) {
                    System.err.println(reply);
                }

                count++;
                if ((count % batchSize) == 0) {
                    epoch++;
                    Instant end = Instant.now();
                    int nanos = Duration.between(start, end).getNano();
                    start = end;

                    double micsPerReq = (double) nanos / batchSize / 1e3;
                    double reqsPerSec = 1e6 / micsPerReq;
                    double mbPerSec = reqsPerSec * payloadSize / 1024 / 1024;

                    System.out.printf("%.0f µs/req - %.1f reqs/s - %.2f mb/s\n", micsPerReq, reqsPerSec, mbPerSec);
                    pw.printf("%d;%f;%f;%f\n", epoch, micsPerReq, reqsPerSec, mbPerSec);
                    pw.flush();
                }
            }
        } finally {
            client.disconnect();
            pw.close();
        }
    }

}

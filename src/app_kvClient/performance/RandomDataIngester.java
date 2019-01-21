package app_kvClient.performance;

import client.KVStore;
import client.exceptions.ClientException;
import common.messages.KVMessage;
import common.utils.HostAndPort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

/**
 * Performance testing utilities: Random data ingestion
 * Arguments: seedNode:port [ms between requests] [payload size in bytes] [number of different keys]
 */
public class RandomDataIngester {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, ClientException {
        if (args.length < 1) {
            throw new RuntimeException("Must at least provide seed address.");
        }
        String[] parts = args[0].split(":");
        HostAndPort server = new HostAndPort(parts[0], Integer.parseInt(parts[1]));

        int sleep = 0;
        if (args.length >= 2) {
            sleep = Integer.parseInt(args[1]);
        }

        int payloadSize = 100000;
        if (args.length >= 3) {
            payloadSize = Integer.parseInt(args[2]);
        }

        int maxKey = 100000;
        if (args.length >= 4) {
            maxKey = Integer.parseInt(args[3]);
        }

        StringBuilder data = new StringBuilder();
        Random rand = new Random();
        rand.ints(payloadSize, 'A', 'z')
                .forEach(c -> data.append((char) c));
        String strData = data.toString();

        KVStore client = new KVStore(server.getHost(), server.getPort());
        client.connect();
        PrintWriter pw = new PrintWriter(new File("data.csv"));
        int batchSize = 10;
        try {
            int count = 0;
            int epoch = 0;
            Instant start = Instant.now();
            while (true) {
                int keyNo = rand.nextInt(maxKey);
                String key = "k" + keyNo;

                KVMessage reply = client.put(key, strData);
                if (reply.getStatus() != KVMessage.StatusType.PUT_SUCCESS
                    && reply.getStatus() != KVMessage.StatusType.PUT_UPDATE) {
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

                Thread.sleep(sleep);
            }
        } finally {
            client.disconnect();
            pw.close();
        }
    }

}

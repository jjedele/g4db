package app_kvClient.performance;

import client.KVStore;
import client.exceptions.ClientException;
import common.messages.KVMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance testing utilities: Data ingestion for the Enron email data set.
 * Arguments: seedNode:port data_root
 */
public class EnronIngester extends SimpleFileVisitor<Path> {

    private final Path root;
    private final KVStore client;
    private final AtomicInteger fileCount;
    private final AtomicLong dataSize;

    public EnronIngester(Path root, KVStore client) {
        this.root = root;
        this.client = client;
        this.fileCount = new AtomicInteger(0);
        this.dataSize = new AtomicLong(0);
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Path relative = root.relativize(file);
        byte[] data = Files.readAllBytes(file);
        fileCount.incrementAndGet();
        dataSize.addAndGet(data.length);
        String content = new String(data, StandardCharsets.UTF_8);
        try {
            Instant start = Instant.now();
            KVMessage reply = client.put(relative.toString(), content);
            Instant end = Instant.now();
            int latency = Duration.between(start, end).getNano();
            System.out.println(latency);
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return super.visitFile(file, attrs);
    }

    public static void main(String[] args) throws IOException, ClientException {
        String[] parts = args[0].split(":");
        KVStore client = new KVStore(parts[0], Integer.parseInt(parts[1]));
        client.connect();
        Path root = Paths.get(args[1]);
        Files.walkFileTree(root, new EnronIngester(root, client));
        client.disconnect();
    }

}

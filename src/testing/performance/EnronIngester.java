package testing.performance;

import client.KVStore;
import client.exceptions.ClientException;
import common.messages.KVMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class EnronIngester extends SimpleFileVisitor<Path> {

    private final Path root;
    private final KVStore client;

    public EnronIngester(Path root, KVStore client) {
        this.root = root;
        this.client = client;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Path relative = root.relativize(file);
        String content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        try {
            KVMessage reply = client.put(relative.toString(), content);
            System.out.println(relative + " " + reply.getStatus());
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return super.visitFile(file, attrs);
    }

    public static void main(String[] args) throws IOException {
        KVStore client = new KVStore("localhost", 50000);
        client.connect();
        Path root = Paths.get("/Volumes/Jeff External SSD/enron_maildir/");
        Files.walkFileTree(root, new EnronIngester(root, client));
        client.disconnect();
    }

}

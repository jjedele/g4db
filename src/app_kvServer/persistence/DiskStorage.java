package app_kvServer.persistence;

import common.utils.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiskStorage implements PersistenceService {

    private static final Logger LOG = LogManager.getLogger(DiskStorage.class);

    private static final byte END_MARKER = (byte) '\n';

    private final File dataDirectory;

    public DiskStorage(File storageDirectory) {
        this.dataDirectory = storageDirectory;
        ensureDataDirectoryExists();
    }

    @Override
    public boolean put(String key, String value) throws PersistenceException {
        File outputFile = escapedFile(key);
        boolean inserted = outputFile.isFile();
        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
            byte[] strToBytes = value.getBytes(StandardCharsets.UTF_8);
            fileOutputStream.write(strToBytes);
            fileOutputStream.write(END_MARKER);
        } catch (IOException e) {
            throw new PersistenceException("Could not put key", e);
        }

        return inserted;
    }

    @Override
    public Optional<String> get(String key) throws PersistenceException {
        File inputFile = escapedFile(key);
        if (!inputFile.exists()) {
            return Optional.empty();
        }

        try (FileInputStream fileInputStream = new FileInputStream(inputFile)) {
            RecordReader reader = new RecordReader(fileInputStream, END_MARKER);
            byte[] read = reader.read();
            return Optional.of(new String(read, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new PersistenceException("Could not read entry for key.", e);
        }
    }

    @Override
    public boolean delete(String key) {
        File inputFile = escapedFile(key);
        if (!inputFile.exists()) {
            return false;
        }

        boolean deleted = inputFile.delete();
        return deleted;
    }

    @Override
    public List<String> getKeys() {
        return Stream.of(dataDirectory.listFiles())
                .map(File::getName)
                .collect(Collectors.toList());
    }

    public boolean contains(String key) {
        File inputFile = escapedFile(key);
        return inputFile.exists();
    }

    private File escapedFile(String key) {
        return new File(dataDirectory, key.replaceAll(File.separator, "_"));
    }

    private void ensureDataDirectoryExists() {
        if (!dataDirectory.isDirectory()) {
            dataDirectory.mkdirs();
        }
    }

}

package app_kvServer.persistence;

import common.utils.RecordReader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiskStorage implements PersistenceService {

    private static final byte END_MARKER = (byte) '\n';

    private final File dataDirectory;

    public DiskStorage(File storageDirectory) {
        this.dataDirectory = storageDirectory;
        ensureDataDirectoryExists();
    }

    @Override
    public boolean put(String key, String value) throws PersistenceException {
        File outputFile = new File(dataDirectory, key);
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
    public String get(String key) throws PersistenceException {
        File inputFile = new File(dataDirectory, key);
        try (FileInputStream fileInputStream = new FileInputStream(inputFile)) {
            RecordReader reader = new RecordReader(fileInputStream, END_MARKER);
            byte[] read = reader.read();
            return new String(read, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new PersistenceException("Could not get key", e);
        }
    }

    @Override
    public void delete(String key) throws PersistenceException {
        File inputFile = new File(dataDirectory, key);
        boolean deleted = inputFile.delete();
        if (!deleted) {
            throw new PersistenceException("Could not delete: " + key);
        }
    }

    @Override
    public List<String> getKeys() throws PersistenceException {
        return Stream.of(dataDirectory.listFiles())
                .map(File::getName)
                .collect(Collectors.toList());
    }

    public boolean contains(String key) {
        File inputFile = new File(dataDirectory, key);
        return inputFile.exists();
    }

    private void ensureDataDirectoryExists() {
        if (!dataDirectory.isDirectory()) {
            dataDirectory.mkdirs();
        }
    }

}

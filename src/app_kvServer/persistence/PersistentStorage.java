package app_kvServer.persistence;

import common.utils.RecordReader;

import java.io.*;
import java.util.Arrays;

public class PersistentStorage implements PersistenceService {

    @Override
    public void persist(String key, String value) throws PersistenceException, IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(key);
        byte[] strToBytes = value.getBytes();
        fileOutputStream.write(strToBytes);
        fileOutputStream.write((byte) '\n');
        fileOutputStream.close();
    }

    @Override
    public String get(String key) throws PersistenceException, IOException {
        File file = new File(key + ".txt");
        FileInputStream fileInputStream = new FileInputStream(file);
        // breaks here
        System.out.println(fileInputStream.available());
        RecordReader reader = new RecordReader(fileInputStream, (byte) '\n');
        byte[] read = reader.read();
        System.out.println(Arrays.toString(read));
        return Arrays.toString(read);
    }

    public boolean contains(String key) {
        return true;
    }

    public static void main(String[] args) throws PersistenceException, IOException {
        PersistentStorage persistentStorage = new PersistentStorage();
        persistentStorage.persist("key", "test1");
        System.out.println(persistentStorage.get("key"));
    }
}

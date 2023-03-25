package app_kvServer;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import app_kvServer.exceptions.FailedException;
import app_kvServer.exceptions.KeyNotFoundException;
import app_kvServer.exceptions.WriteLockException;

import java.io.*;

import shared.messages.KVMessage.StatusType;

class Cache extends Thread {

    LinkedHashMap<String, String> cache;
    private boolean is_locked = false;
    String dir;

    public Cache(final int cacheSize, String hostname, int port) {
        cache = new LinkedHashMap<String, String>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > cacheSize;
            }
        };
        dir = String.format("./.cache/%s:%s", hostname, port);
        createFolderIfNotExist(dir);
    }

    public Cache(final int cacheSize, String hostname, int port, String replica_hostname, int replicate_port) {
        cache = new LinkedHashMap<String, String>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > cacheSize;
            }
        };
        dir = String.format("./.cache/%s:%s/%s:%s", hostname, port, replica_hostname, replicate_port);
        createFolderIfNotExist(dir);
    }

    private void createFolderIfNotExist(String path) {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdir();
        }
    }

    private String getFilepath(String key) {
        return String.format("%s/%s", dir, key);
    }

    private void saveToDisk(String key, String value) throws WriteLockException {
        if (is_locked) {
            throw new WriteLockException();
        }
        String filepath = getFilepath(key);
        File file = new File(filepath);
        file.delete();
        try {
            FileWriter writer = new FileWriter(filepath);
            writer.write(value);
            writer.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public boolean onDisk(String key) {
        File file = new File(getFilepath(key));
        return file.exists();
    }

    public LinkedHashMap<String, String> getAllKeyValues() {
        File folder = new File(dir);
        File[] listOfFiles = folder.listFiles();

        if (listOfFiles == null) {
            return null;
        }

        LinkedHashMap<String, String> values = new LinkedHashMap<String, String>();
        for (File file : listOfFiles) {
            try {
                values.put(file.getName(), findFromDisk(file.getName()));
            } catch (Exception e) {
                System.out.println("Error getting all key values in Cache class");
            }
        }
        return values;
    }

    // TODO MAKE THIS SYNCHRONIZED??
    private String findFromDisk(String key) throws FailedException {
        String filepath = getFilepath(key);
        File file = new File(filepath);
        try {
            Scanner reader = new Scanner(file);
            String value = reader.nextLine();
            reader.close();
            return value;
        } catch (FileNotFoundException e) {
            throw new FailedException();
        }
    }

    public void setWriteLock(boolean locked) {
        this.is_locked = locked;
    }

    public boolean getWriteLock() {
        return this.is_locked;
    }

    public StatusType save(String key, String value) throws WriteLockException {
        if (this.is_locked) {
            throw new WriteLockException();
        }

        StatusType status = StatusType.PUT;
        if (cache.containsKey(key)) {
            cache.remove(key);
            status = StatusType.PUT_UPDATE;
        }
        cache.put(key, value);
        saveToDisk(key, value);

        return status;
    }

    public String find(String key) throws KeyNotFoundException, FailedException {
        if (cache.containsKey(key)) {
            return cache.get(key);
        }
        if (onDisk(key)) {
            return findFromDisk(key);
        }
        throw new KeyNotFoundException();
    }

    public void delete(String key, boolean forceDelete) throws KeyNotFoundException, WriteLockException {
        if (this.is_locked && !forceDelete) {
            throw new WriteLockException();
        }

        if (!containsKey(key)) {
            throw new KeyNotFoundException();
        }

        if (cache.containsKey(key)) {
            cache.remove(key);
        }

        String filepath = getFilepath(key);

        File file = new File(filepath);
        if (file.exists()) {
            file.delete();
        }

    }

    public boolean containsKey(String key) {
        return cache.containsKey(key) || onDisk(key);
    }

    public void clearCache() throws WriteLockException {
        if (this.is_locked) {
            throw new WriteLockException();
        }
        cache.clear();
    }

    public void clearDisk(boolean force_delete) throws WriteLockException {
        if (this.is_locked && !force_delete) {
            throw new WriteLockException();
        }
        File dir = new File(".cache");
        for (File file : dir.listFiles())
            if (!file.isDirectory())
                file.delete();
    }

    public void printCache() {
        System.out.println(cache);
    }
}
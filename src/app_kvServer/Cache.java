package app_kvServer;

import java.util.*;
import java.io.*;

import shared.messages.KVMessage.StatusType;

//TODO real delete functionality
//TODO move into KVServer
class Cache extends Thread {

    LinkedHashMap<String, String> cache;

    public Cache(final int cacheSize) {
        cache = new LinkedHashMap<String, String>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > cacheSize;
            }
        };
    }

    private String getFilepath(String key) {
        return String.format("./.cache/%s", key);
    }

    // TODO MAKE THIS SYNCHRONIZED??
    private void saveToDisk(String key, String value) {
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

    // TODO MAKE THIS SYNCHRONIZED??
    private String findFromDisk(String key) {
        String filepath = getFilepath(key);
        File file = new File(filepath);
        try {
            Scanner reader = new Scanner(file);
            String value = reader.nextLine();
            reader.close();
            return value;
        } catch (FileNotFoundException e) {
            return " "; // TODO Change this to something better
        }
    }

    public StatusType save(String key, String value) {
        StatusType status = StatusType.PUT;
        if (cache.containsKey(key)) {
            cache.remove(key);
            status = StatusType.PUT_UPDATE;

        }
        cache.put(key, value);
        saveToDisk(key, value);

        return status;
    }

    public String find(String key) throws Exception {
        if (cache.containsKey(key)) {
            System.out.println("Found in cache");
            return cache.get(key);
        }
        if (onDisk(key)) {
            return findFromDisk(key);
        }
        throw new Exception("Could not find key");
    }

    public void delete(String key) throws Exception {
        if (!containsKey(key)) {
            throw new Exception("Could not find key");
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

    public void clearCache() {
        cache.clear();
    }

    public void clearDisk() {
        File dir = new File(".cache");
        for (File file : dir.listFiles())
            if (!file.isDirectory())
                file.delete();
    }

    public void printCache() {
        System.out.println(cache);
    }

    // TODO: THIS IS TEST CODE?
    // public void run() {
    // save("1", "A");
    // save("2", "B");
    // save("3", "C");
    // save("1", "D");
    // save("4", "E");
    // printCache();
    // System.out.println(find("2"));
    // }

    // public static void main(String[] args) {
    // new Cache(3).start();
    // System.out.println("hello world");
    // }
}
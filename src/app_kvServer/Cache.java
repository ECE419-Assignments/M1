package app_kvServer;

import java.util.*;
import java.io.*;

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

    private String findFromDisk(String key) {
        String filepath = getFilepath(key);
        File file = new File(filepath);
        try {
            Scanner reader = new Scanner(file);
            String value = reader.nextLine();
            reader.close();
            return value;
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
        return ":::not_found";
    }

    public void save(String key, String value) {
        if (cache.containsKey(key)) {
            cache.remove(key);
        }
        cache.put(key, value);
        saveToDisk(key, value);
    }

    public String find(String key) {
        if (cache.containsKey(key)) {
            return cache.get(key);
        }
        if (onDisk(key)) {
            return findFromDisk(key);
        }
        return ":::not_found";
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

    public void run() {
        save("1", "A");
        save("2", "B");
        save("3", "C");
        save("1", "D");
        save("4", "E");
        printCache();
        System.out.println(find("2"));
    }

    public static void main(String[] args) {
        new Cache(3).start();
        System.out.println("hello world");
    }
}
package app_kvServer;

import app_kvServer.exceptions.ServerStoppedException;
import app_kvServer.exceptions.WriteLockException;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    /**
     * Get the port number of the server
     * 
     * @return port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * 
     * @return hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * 
     * @return cache strategy
     */
    public CacheStrategy getCacheStrategy() throws ServerStoppedException;

    /**
     * Get the cache size
     * 
     * @return cache size
     */
    public int getCacheSize() throws ServerStoppedException;

    public String[] getNodeHashRange() throws ServerStoppedException;

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * 
     * @return true if key in storage, false otherwise
     */
    public boolean inStorage(String key) throws ServerStoppedException;

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * 
     * @return true if key in storage, false otherwise
     */
    public boolean inMainCache(String key) throws ServerStoppedException;

    /**
     * Get the value associated with the key
     * 
     * @return value associated with key
     * @throws Exception
     *                   when key not in the key range of the server
     */
    public String getKV(String key) throws Exception, ServerStoppedException;

    /**
     * Put the key-value pair into storage
     * 
     * @throws Exception
     *                   when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception, ServerStoppedException;

    /**
     * Clear the local cache of the server
     */
    public void clearCache() throws ServerStoppedException, WriteLockException;

    /**
     * Clear the storage of the server
     */
    public void clearStorage() throws ServerStoppedException, WriteLockException;

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();
}

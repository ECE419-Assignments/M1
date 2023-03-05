public
package ecs;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;

public class ECSNode extends KVServer implements IECSNode {

    String host;
    int port;
    ECSClient ECSMaster;

    public ECSNode(ECSClient ECSMaster, String host, int port, int cacheSize, CacheStrategy cacheStrategy) {
        super(port, cacheSize, cacheStrategy);
        this.host = host;
        this.port = port;
        this.ECSMaster = ECSMaster;
    }

    @Override
    public String getNodeName() {
        return null;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    public void updateMetadata(String[] metadata) { // Send over the string
        this.hash_range = hash_range;
    }

    public void moveValuesToCorrectServer() { // TODO: Navid
        this.setWriteLock(true);

        // cache_key_values = all_cache_key_values

        // for ( String key, String value) in cache_key_values {
        // ECSNode targetServer = ECSMaster.getNodeByKey(key);
        // if (this.getName() != targetServer.getName()) {
        // targetServer.putKV(key, value);
        // this.deleteKV(key);
        // }
        // }

        this.setWriteLock(false);
    }

    public void moveKVsToServer(ECSNode targetServer) { // TODO: Navid
        this.setWriteLock(true);

        // cache_key_values = all_cache_key_values

        // for ( String key, String value) in cache_key_values {
        // targetServer.putKV(key, value);
        // this.deleteKV(key);
        // }

        this.setWriteLock(false);
    }

    public void killServer() {
        this.kill();
    }
}{

}

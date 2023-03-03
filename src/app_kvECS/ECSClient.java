package app_kvECS;

import java.util.Map;
import java.util.TreeMap;

import app_kvServer.IKVServer.CacheStrategy;

import java.util.Collection;

import ecs.ECSNode;
import ecs.ECSNode;

// TODO: Navid - Update metadata on server delete, add and so on
public class ECSClient implements IECSClient {

    TreeMap<String, ECSNode> server_tree = new TreeMap();

    private int current_port = 50000;

    @Override
    public boolean start() {
        try {
            for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
                ECSNode server = server_entry.getValue();
                server.startServer();
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean stop() {
        try {
            for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
                ECSNode server = server_entry.getValue();
                server.stopServer();
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        try {
            for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
                ECSNode server = server_entry.getValue();
                server.close();
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize) {
        String address = String.format("localhost:%o", current_port);
        server_tree.put(address, new ECSNode(this, "localhost", current_port, cacheSize, CacheStrategy.FIFO));
        current_port += 1;

        return null;
    }

    @Override
    public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        for (int i = 0; i < count; i++) {
            addNode(cacheStrategy, cacheSize);
        }

        return null;
    }

    public void moveValuesToCorrectServers() {
        for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
            ECSNode server = server_entry.getValue();
            server.moveValuesToCorrectServer();
        }
    }

    @Override
    public Collection<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {

        addNodes(count, cacheStrategy, cacheSize);
        moveValuesToCorrectServers();
        start();

        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception { // Navid
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) { // Navid
        // ECSNode targetServer;
        // for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
        // ECSNode server = server_entry.getValue();
        // if (server.getName() not in nodeNames) {
        // targetServer = server;
        // break;
        // }
        // }

        // if (targetServer == null) {
        // return false;
        // }

        // for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
        // ECSNode server = server_entry.getValue();
        // if (server.getName() in nodeNames) {
        // server.moveKVsToServer(targetServer);
        // server.killServer();
        // }
        // }

        // moveValuesToCorrectServers();
        // return true;
        return false;
    }

    @Override
    public Map<String, ECSNode> getNodes() { // Navid
        // TODO
        return null;
    }

    @Override
    public ECSNode getNodeByKey(String Key) { // Zeni
        // TODO
        return null;
    }

    public static void main(String[] args) { // Zeni
        // TODO
    }
}

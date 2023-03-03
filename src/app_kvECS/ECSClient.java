package app_kvECS;

import java.util.Map;
import java.util.TreeMap;

import app_kvServer.IKVServer.CacheStrategy;

import java.util.Collection;

import ecs.ECSNode;
import ecs.ECSNode;

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

        // TODO: Noramlize data between servers
        return null;
    }

    @Override
    public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        for (int i = 0; i < count; i++) {
            addNode(cacheStrategy, cacheSize);
        }

        // TODO: Noramlize data between servers
        return null;
    }

    @Override
    public Collection<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        addNodes(count, cacheStrategy, cacheSize);

        for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
            ECSNode server = server_entry.getValue();
            server.moveValuesToCorrectServer();
        }

        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, ECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public ECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public static void main(String[] args) {
        // TODO
    }
}

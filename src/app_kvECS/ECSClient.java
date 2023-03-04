package app_kvECS;

import java.util.Map;
import java.util.TreeMap;

import app_kvServer.IKVServer.CacheStrategy;

import java.util.Collection;
import java.util.Collections;

import ecs.ECSNode;
import shared.KVHasher;

// TODO: Zeni - Double check and make sure all the KVHasher stuff here is correct. I'm not sure what format
// you used for the server_info so you might need to change the places where I pass server_info
public class ECSClient implements IECSClient {

    TreeMap<String, ECSNode> server_tree = new TreeMap();

    KVHasher kvHasher = new KVHasher();

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
                server.killServer();
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private void moveValuesToCorrectServers() {
        for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
            ECSNode server = server_entry.getValue();
            server.moveValuesToCorrectServer();
        }
    }

    private void updateNodeHashRanges() {
        for (Map.Entry<String, ECSNode> entry : server_tree.entrySet()) {
            String key = entry.getKey();
            ECSNode server_node = entry.getValue();

            server_node.updateNodeHashRanges(kvHasher.getServerHashRange(server_tree, key));
            server_tree.put(key, server_node);
        }
        moveValuesToCorrectServers();
    }

    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize, boolean stopStartServer) {
        if (stopStartServer) {
            stop();
        }

        String address = String.format("localhost:%o", current_port);

        ECSNode node = new ECSNode(this, "localhost", current_port, cacheSize, CacheStrategy.FIFO);
        // TODO: Zeni - Get server_info from ip and port
        server_tree = kvHasher.addServer(server_tree, server_info, node);
        updateNodeHashRanges();

        current_port += 1;
        if (stopStartServer) {
            start();
        }
        return node;
    }

    @Override
    public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize, boolean stopStartServer) {
        Collection<ECSNode> nodes = Collections.emptyList();

        for (int i = 0; i < count; i++) {
            nodes.add(addNode(cacheStrategy, cacheSize, stopStartServer));
        }

        return nodes;
    }

    @Override
    public Collection<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        addNodes(count, cacheStrategy, cacheSize, false);
        start();

        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception { // Navid
        // TODO
        return false;
    }

    public boolean removeNode(String nodeName) { // Navid
        stop();

        boolean removeSuccessful = false;
        for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
            ECSNode server = server_entry.getValue();
            if (server.getName() == nodeName) {
                // TODO: Zeni - Get server info here
                server_tree = kvHasher.deleteServer(server_tree, server_info);
                updateNodeHashRanges();
                server.killServer();

                removeSuccessful = true;
                break;
            }
        }
        start();
        return removeSuccessful;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) { // Navid
        for (String nodeName : nodeNames) {
            this.removeNode(nodeName);
        }

        return true;
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
        // Add
        // Delete
        // Start
        // Stop
        // Shutdown
    }
}

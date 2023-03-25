package app_kvECS;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.naming.NameNotFoundException;

import app_kvServer.IKVServer.CacheStrategy;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import shared.ecs.ECSNode;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;
import shared.metadata.KVMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

// TODO: Zeni - Double check and make sure all the KVHasher stuff here is correct. I'm not sure what format
// you used for the server_info so you might need to change the places where I pass server_info
public class ECSClient extends Thread implements IECSClient {

    private static Logger logger = Logger.getLogger("ECS Client");
    TreeMap<String, ECSNode> server_tree = new TreeMap();

    public volatile KVMetadata kvMetadata = new KVMetadata();

    private boolean running;
    private ServerSocket serverSocket;
    private int port;

    public Set<ServerConnection> serverConnections;

    public void close() {
        logger.info("Closing ECS Server!");
        this.running = false;
        System.exit(0);
    }

    public ECSClient(int port) {
        this.port = port;
        try {
            serverConnections = new HashSet<ServerConnection>();
            new LogSetup("logs/ecs.log", Level.ALL);
        } catch (IOException e) {
            System.out.println("error setting sup ecs logger");
        }
        this.start();
    }

    // @Override
    // public boolean start() {
    // // try {
    // // for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
    // // ECSNode server = server_entry.getValue();
    // // server.startServer();
    // // }
    // // } catch (Exception e) {
    // // return false;
    // // }
    // // return true;
    // return false;
    // }

    // @Override
    // public boolean stop() {
    // // try {
    // // for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
    // // ECSNode server = server_entry.getValue();
    // // server.stopServer();
    // // }
    // // } catch (Exception e) {
    // // return false;
    // // }
    // // return true;
    // return false;
    // }

    @Override
    public boolean shutdown() {
        // try {
        // for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
        // ECSNode server = server_entry.getValue();
        // server.killServer();
        // }
        // } catch (Exception e) {
        // return false;
        // }
        // return true;
        return false;
    }

    private void moveValuesToCorrectServers() {
        // for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
        // ECSNode server = server_entry.getValue();
        // server.moveValuesToCorrectServer();
        // }
    }

    private void updateNodeHashRanges() {
        for (Map.Entry<String, ECSNode> entry : server_tree.entrySet()) {
            String key = entry.getKey();
            ECSNode server_node = entry.getValue();

            // server_node.updateNodeHashRanges(kvHasher.getServerHashRange(server_tree,
            // key));
            server_tree.put(key, server_node);
        }
        moveValuesToCorrectServers();
    }

    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize, boolean stopStartServer) {
        // if (stopStartServer) {
        // stop();
        // }

        // String address = String.format("localhost:%o", current_port);

        // ECSNode node = new ECSNode(this, "localhost", current_port, cacheSize,
        // CacheStrategy.FIFO);
        // // TODO: Zeni - Get server_info from ip and port
        // server_tree = kvHasher.addServer(server_tree, server_info, node);
        // updateNodeHashRanges();

        // current_port += 1;
        // if (stopStartServer) {
        // start();
        // }
        // return node;
        return null;
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
        // addNodes(count, cacheStrategy, cacheSize, false);
        // run();

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
        // for (Map.Entry<String, ECSNode> server_entry : server_tree.entrySet()) {
        // ECSNode server = server_entry.getValue();
        // if (server.getName() == nodeName) {
        // // TODO: Zeni - Get server info here
        // // server_tree = kvHasher.deleteServer(server_tree, server_info);
        // updateNodeHashRanges();
        // server.killServer();

        // removeSuccessful = true;
        // break;
        // }
        // }
        // start();
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

    public void updateAllServerMetadatas() {
        for (ServerConnection connection : serverConnections) {
            try {
                connection.sendMessage(new KVM(StatusType.UPDATE_METADATA, "", kvMetadata.getKeyRange()));
            } catch (Exception e) {
                System.out.println("Error in update all server metadatas");
            }
        }
    }

    public void run() {
        running = initializeECSClient();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    logger.info("opening connection");
                    Socket server = serverSocket.accept();
                    ServerConnection connection = new ServerConnection(this, server);
                    serverConnections.add(connection);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + server.getInetAddress().getHostName()
                            + " on port " + server.getPort());
                } catch (IOException e) {
                    logger.info("Error! " +
                            "Unable to establish connection. \n");
                }
            }
            logger.info("done with while");
        }
    }

    private boolean isRunning() {
        return this.running;
    }

    public boolean initializeECSClient() {

        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);

            logger.info("ECS Client listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    public ServerConnection getServerConnectionWithAddress(String address) throws NameNotFoundException {
        for (ServerConnection serverConnection : serverConnections) {
            if (serverConnection.address.equals(address)) {
                return serverConnection;
            }
        }

        throw new NameNotFoundException();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error! Invalid number of arguments!");
            System.out.println("Usage: ECS <port>!");
        }
        int port = Integer.parseInt(args[0]);
        new ECSClient(port);
    }

    // Extra for add node
    // public addNode() {
    // host = "localhost"
    // port = 50051
    // server_address = "localhost:50051"
    // KVMetadataNode node = KVMetadata.add_server(host, port);
    // // Add a new attribute for prevNode, nextNode
    // if (node == node.prevNode) {
    // return;
    // }

    // node.updateMetadata();

    // }
}

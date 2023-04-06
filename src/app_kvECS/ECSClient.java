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
    private String primeEcsAddress;

    public Set<ServerConnection> serverConnections;
    public ServerConnection backupEcsConnection;

    ECSToECSConnection primeEcsConnection;

    public void close() {
        logger.info("Closing ECS Server!");
        this.running = false;
        System.exit(0);
    }

    public ECSClient(int port, String primeEcsAddress, boolean isBackup) {
        this.port = port;
        this.primeEcsAddress = primeEcsAddress;
        try {
            serverConnections = new HashSet<ServerConnection>();
            new LogSetup("logs/ecs.log", Level.ALL);
        } catch (IOException e) {
            System.out.println("error setting sup ecs logger");
        }
        // if (!isBackup) {
        // new ECSClient(port + 1, String.format("localhost:%d", port), true);
        // }
        this.start();
    }

    public String getHostname() {
        return "127.0.0.1";
    }

    public int getPort() {
        return this.port;
    }

    public void switchToPrimeEcs() {
        this.primeEcsAddress = "";
    }

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

    public void sendUpdateAllServerBackupEcsAddresses() {
        System.out.println("updating all server backup ecs addresses");
        for (ServerConnection connection : serverConnections) {
            try {
                System.out.println(this.backupEcsConnection);
                if (this.backupEcsConnection != null) {
                    System.out.println("sending the ecs address");
                    connection.sendMessage(
                            new KVM(StatusType.SET_BACKUP_ECS_ADDRESS, "", this.backupEcsConnection.address));
                }
            } catch (Exception e) {
                System.out.println("Error in update all server metadatas");
            }
        }
    }

    public void updateAllServerReplicas() {
        for (ServerConnection connection : serverConnections) {
            try {
                connection.sendMessage(new KVM(StatusType.UPDATE_REPLICAS, " S", " "));
            } catch (Exception e) {
                System.out.println("Error in update all server replicas");
            }
        }
    }

    public void addServerConnection(ServerConnection connection) {
        serverConnections.add(connection);
    }

    public void addBackupEcsConnection(ServerConnection connection) {
        backupEcsConnection = connection;
    }

    public void updateBackupEcsMetadata() throws IOException {
        if (this.backupEcsConnection != null) {
            this.backupEcsConnection.sendUpdateMetadataMessage();
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

            // Start ECS Socket
            if (primeEcsAddress != "") {
                this.primeEcsConnection = new ECSToECSConnection(this, primeEcsAddress);
                new Thread(this.primeEcsConnection).start();
            }

            logger.info("ECS Client listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (Exception e) {
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
        if (args.length != 1 && args.length != 2) {
            System.out.println("Error! Invalid number of arguments!");
            System.out.println("Usage: ECS <port> <prime ecs address: optional>!");
        }
        String primeEcsAddress = "";
        int port = Integer.parseInt(args[0]);
        if (args.length == 2) {
            primeEcsAddress = args[1];
        }
        new ECSClient(port, primeEcsAddress, false);
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

package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;

import shared.ecs.ECSNode;
import shared.BaseConnection;
import shared.misc;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;
import shared.metadata.KVMetadata;

public class ECSConnection extends BaseConnection {

    protected KVServer kvServer;

    public ECSConnection(KVServer kvServer, String host, int port) throws IOException, InterruptedException {
        super(host, port);
        this.kvServer = kvServer;

        Thread.sleep(100);
    }

    @Override()
    public void postStart() throws IOException {
        sendMessage(new KVM(StatusType.NEW_SERVER, " ",
                String.format("%s:%s", kvServer.getHostname(), kvServer.getPort())));
    }

    public void serverShuttingDown() throws IOException {
        sendMessage(new KVM(StatusType.SERVER_SHUTDOWN, " ",
                String.format("%s:%s", kvServer.getHostname(), kvServer.getPort())));
    }

    @Override()
    public void processMessage(KVM message) throws IOException {
        StatusType status = message.getStatus();
        String key = message.getKey();
        String value = message.getValue();
        logger.info(status);

        StatusType responseStatus = StatusType.FAILED;
        String responseKey = key;
        String responseValue = value;
        boolean sendResponse = false;

        try {
            if (status.equals(StatusType.TOGGLE_WRITE_LOCK)) {
                this.kvServer.setWriteLock(!this.kvServer.getWriteLock());
                responseStatus = StatusType.TOGGLE_WRITE_LOCK_SUCCESS;
                sendResponse = true;
            } else if (status.equals(StatusType.START_SERVER)) {
                this.kvServer.startServer();
            } else if (status.equals(StatusType.CLOSE_LAST_SERVER)) {
                this.kvServer.close();
                this.close();
            } else if (status.equals(StatusType.UPDATE_METADATA)) {
                this.kvServer.metadata.createServerTree(value);
            } else if (status.equals(StatusType.SEND_ALL_DATA_TO_PREV)) {
                System.out.println("deleting server");
                String server_address = value;
                String host = misc.getHostFromAddress(server_address);
                int port = misc.getPortFromAddress(server_address);
                Socket socket = new Socket(host, port); // TODO: Navid
                ClientConnection connection = new ClientConnection(this.kvServer, socket);
                new Thread(connection).start();

                LinkedHashMap<String, String> values = this.kvServer.getAllKeyValues();
                System.out.println(values);

                if (!(values == null)) {
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        System.out.println("sending keys");
                        String cur_key = entry.getKey();
                        String cur_val = entry.getValue();
                        connection.sendMessage(new KVM(StatusType.PUT, cur_key, cur_val));
                    }
                }

                Thread.sleep(100);
                connection.close();
                this.sendMessage(new KVM(StatusType.DATA_MOVED_CONFIRMATION_SHUTDOWN, " ", " "));

                if (!(values == null)) {
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        String cur_key = entry.getKey();
                        this.kvServer.deleteKV(cur_key, true);
                    }
                }
                this.kvServer.close();
                this.close();

            } else if (status.equals(StatusType.SEND_FILTERED_DATA_TO_NEXT)) {
                System.out.println("adding server");
                String server_address = value, keyrange = key;

                KVMetadata kvMetadata = new KVMetadata();
                kvMetadata.createServerTree(keyrange);

                String host = misc.getHostFromAddress(server_address);
                int port = misc.getPortFromAddress(server_address);
                Socket socket = new Socket(host, port);

                ClientConnection connection = new ClientConnection(this.kvServer, socket);
                new Thread(connection).start();
                Thread.sleep(500);

                LinkedHashMap<String, String> values = this.kvServer.getAllKeyValues();
                System.out.println(values);

                if (!(values == null)) {
                    System.out.println("sending keys");
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        String cur_key = entry.getKey();
                        String cur_val = entry.getValue();
                        shared.ecs.ECSNode correctServerNode = kvMetadata.getKeysServer(cur_key);
                        if (server_address.equals(correctServerNode.getNodeAddress())) {
                            connection.sendMessage(new KVM(StatusType.PUT, cur_key, cur_val));
                        }
                    }
                }

                Thread.sleep(100);
                connection.close();

                if (!(values == null)) {
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        String cur_key = entry.getKey();
                        String cur_val = entry.getValue();
                        shared.ecs.ECSNode correctServerNode = kvMetadata.getKeysServer(cur_key);
                        if (server_address.equals(correctServerNode.getNodeAddress())) {
                            this.kvServer.deleteKV(cur_key, true);
                        }
                    }
                }

                Thread.sleep(100);
                this.sendMessage(new KVM(StatusType.DATA_MOVED_CONFIRMATION_NEW, " ", " "));
            }
        } catch (Exception e) {
            System.out.println(e);
            responseStatus = StatusType.FAILED;
            responseValue = e.getMessage();
        }

        if (sendResponse) {
            this.sendMessage(new KVM(responseStatus, responseKey, responseValue));
        }
    }

}

package app_kvServer;

import java.io.IOException;
import java.net.Socket;

import ecs.ECSNode;
import shared.BaseConnection;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;
import shared.metadata.KVMetadata;

public class ECSConnection extends BaseConnection {

    protected KVServer kvServer;

    public ECSConnection(KVServer kvServer, String host, int port) throws IOException {
        super(host, port);
        this.kvServer = kvServer;
        sendMessage(new KVM(StatusType.NEW_SERVER, "", ""));
    }

    public void serverShuttingDown() throws IOException {
        sendMessage(new KVM(StatusType.SERVER_SHUTDOWN, "", ""));
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
            } else if (status.equals(StatusType.UPDATE_METADATA)) {
                this.kvServer.metadata.createServerTree(value);
            } else if (status.equals(StatusType.SEND_ALL_DATA_TO_PREV)) { // Delete Server
                // this.server.sendAllDataToServer(node);
                // this.sendDataMovedConfirmation();
                // this.server.deleteAllData();
                // this.server.shutdown();
            } else if (status.equals(StatusType.SEND_FILTERED_DATA_TO_NEXT)) { // New Server
                // Send data to server on value address
                sendMessage(new KVM(StatusType.DATA_MOVED_CONFIRMATION_SHUTDOWN, "", ""));
            } else if (status.equals(StatusType.SEND_FILTERED_DATA_TO_NEXT)) {
                String server_address = value, keyrange = key;

                KVMetadata kvMetadata = new KVMetadata();
                kvMetadata.createServerTree(keyrange);

                Socket socket = new Socket(String.split(":", server_address)); // TODO: Navid
                ClientConnection connection = new ClientConnection(this.kvServer, socket);
                new Thread(connection).start();

                keys, values = get_all_keys_values();

                foreach key, value {
                    shared.ecs.ECSNode correctServerNode = kvMetadata.getKeysServer(key);
                    if (server_address == correctServerNode.getNodeAddress()) {
                        connection.sendMessage(new KVM(StatusType.PUT, key, value));
                    }

                }
            }
        } catch (Exception e) {
            responseStatus = StatusType.FAILED;
            responseValue = e.getMessage();
        }

        if (sendResponse) {
            this.sendMessage(new KVM(responseStatus, responseKey, responseValue));
        }
    }

}

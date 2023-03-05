package app_kvECS;

import java.io.IOException;
import java.net.Socket;

import app_kvServer.exceptions.KeyNotFoundException;
import app_kvServer.exceptions.ServerNotResponsibleException;
import app_kvServer.exceptions.ServerStoppedException;
import app_kvServer.exceptions.WriteLockException;
import shared.ecs.ECSNode;
import shared.BaseConnection;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;

public class ServerConnection extends BaseConnection {

    private ECSClient ecsClient;

    public String address;

    public ServerConnection(ECSClient ecsClient, Socket socket) {
        super(socket);
        this.ecsClient = ecsClient;
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
            if (status.equals(StatusType.NEW_SERVER)) {
                address = value;
                logger.info("Hello 0");
                this.ecsClient.kvMetadata.addServer(value);

                logger.info("Hello 1");
                this.sendMessage(new KVM(StatusType.UPDATE_METADATA, " ", this.ecsClient.kvMetadata.getKeyRange()));

                this.sendMessage(new KVM(StatusType.START_SERVER, " ", ""));

                logger.info("Hello 2");
                ECSNode prevNode = this.ecsClient.kvMetadata.getSuccesorNode(value);
                logger.info(String.format("Updating prev metadata for %s. The node being added is %s",
                        prevNode.getNodeAddress(), value));

                logger.info("Hello 3");
                if (this.ecsClient.kvMetadata.getCountServers() != 1) {
                    logger.info("Hello 3.5");
                    ServerConnection prevConnection = this.ecsClient
                            .getServerConnectionWithAddress(prevNode.getNodeAddress());
                    logger.info("Hello 4");

                    prevConnection.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, " ", " "));
                    logger.info("Hello 5");
                    Thread.sleep(500);
                    prevConnection.sendMessage(new KVM(StatusType.SEND_FILTERED_DATA_TO_NEXT,
                            this.ecsClient.kvMetadata.getKeyRange(), this.address));
                    logger.info("Hello 6");
                }
            } else if (status.equals(StatusType.SERVER_SHUTDOWN)) {
                logger.info(String.format("Deleting server with address %s", value));
                this.ecsClient.kvMetadata.deleteServer(value);
                this.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, " ", " "));
                ECSNode prevNode = this.ecsClient.kvMetadata.getSuccesorNode(value);
                logger.info(String.format("Updating prev metadata for %s. The node being deleted is %s",
                        prevNode.getNodeAddress(), value));
                ServerConnection prevConnection = this.ecsClient
                        .getServerConnectionWithAddress(prevNode.getNodeAddress());
                prevConnection
                        .sendMessage(new KVM(StatusType.UPDATE_METADATA, " ", this.ecsClient.kvMetadata.getKeyRange()));
                Thread.sleep(10);
                this.sendMessage(new KVM(StatusType.SEND_ALL_DATA_TO_PREV, " ", prevConnection.address));

            } else if (status.equals(StatusType.DATA_MOVED_CONFIRMATION_NEW)) {
                this.ecsClient.updateAllServerMetadatas();
                sendResponse = false;
                Thread.sleep(10);
                this.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, " ", " "));
            } else if (status.equals(StatusType.DATA_MOVED_CONFIRMATION_SHUTDOWN)) {
                this.ecsClient.updateAllServerMetadatas();
                sendResponse = false;
                // TODO: Navid - Delete this connection
                this.close();
                this.ecsClient.serverConnections.remove(this);
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

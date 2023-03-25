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
    public void postClosed() {
        logger.info(String.format("Deleting server with address %s", this.address));
        this.ecsClient.kvMetadata.deleteServer(this.address);

        if (this.ecsClient.kvMetadata.getCountServers() != 0) {
            ECSNode prevNode = this.ecsClient.kvMetadata.getSuccesorNode(this.address);
            logger.info(String.format("Updating prev metadata for %s. The node being deleted is %s",
                    prevNode.getNodeAddress(), this.address));
            ServerConnection prevConnection = this.ecsClient
                    .getServerConnectionWithAddress(prevNode.getNodeAddress());
            prevConnection
                    .sendMessage(
                            new KVM(StatusType.UPDATE_METADATA, " ", this.ecsClient.kvMetadata.getKeyRange()));
            Thread.sleep(10);
            this.sendMessage(new KVM(StatusType.SEND_ALL_DATA_TO_PREV, " ", prevConnection.address));
        } else {
            Thread.sleep(100);
            this.sendMessage(new KVM(StatusType.CLOSE_LAST_SERVER, "", ""));
        }
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
                this.ecsClient.kvMetadata.addServer(value);
                this.sendMessage(new KVM(StatusType.UPDATE_METADATA, " ", this.ecsClient.kvMetadata.getKeyRange()));

                this.sendMessage(new KVM(StatusType.START_SERVER, " ", ""));

                // This should be in the if statement I think
                ECSNode prevNode = this.ecsClient.kvMetadata.getSuccesorNode(value);
                logger.info(String.format("Updating prev metadata for %s. The node being added is %s",
                        prevNode.getNodeAddress(), value));

                if (this.ecsClient.kvMetadata.getCountServers() != 1) {
                    ServerConnection prevConnection = this.ecsClient
                            .getServerConnectionWithAddress(prevNode.getNodeAddress());

                    prevConnection.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, " ", " "));
                    Thread.sleep(500);
                    prevConnection.sendMessage(new KVM(StatusType.SEND_FILTERED_DATA_TO_NEXT,
                            this.ecsClient.kvMetadata.getKeyRange(), this.address));
                }
            } else if (status.equals(StatusType.SERVER_SHUTDOWN)) {
                logger.info(String.format("Deleting server with address %s", value));
                this.ecsClient.kvMetadata.deleteServer(value);
                this.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, " ", " "));

                if (this.ecsClient.kvMetadata.getCountServers() != 0) {
                    ECSNode prevNode = this.ecsClient.kvMetadata.getSuccesorNode(value);
                    logger.info(String.format("Updating prev metadata for %s. The node being deleted is %s",
                            prevNode.getNodeAddress(), value));
                    ServerConnection prevConnection = this.ecsClient
                            .getServerConnectionWithAddress(prevNode.getNodeAddress());
                    prevConnection
                            .sendMessage(
                                    new KVM(StatusType.UPDATE_METADATA, " ", this.ecsClient.kvMetadata.getKeyRange()));
                    Thread.sleep(10);
                    this.sendMessage(new KVM(StatusType.SEND_ALL_DATA_TO_PREV, " ", prevConnection.address));
                } else {
                    Thread.sleep(100);
                    this.sendMessage(new KVM(StatusType.CLOSE_LAST_SERVER, "", ""));
                }
            } else if (status.equals(StatusType.DATA_MOVED_CONFIRMATION_NEW)) {
                this.ecsClient.updateAllServerMetadatas();
                this.ecsClient.updateAllServerReplicas();
                sendResponse = false;
                Thread.sleep(10);
                this.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, " ", " "));
            } else if (status.equals(StatusType.DATA_MOVED_CONFIRMATION_SHUTDOWN)) {
                this.ecsClient.updateAllServerMetadatas();
                this.ecsClient.updateAllServerReplicas();
                sendResponse = false;
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

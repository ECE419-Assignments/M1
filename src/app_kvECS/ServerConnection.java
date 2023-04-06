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

        try {
            ECSNode prevNode = this.ecsClient.kvMetadata.getSuccesorNode(this.address);
            logger.info(String.format("Updating prev metadata for %s. The node being deleted is %s",
                    prevNode.getNodeAddress(), this.address));
            ServerConnection prevConnection = this.ecsClient
                    .getServerConnectionWithAddress(prevNode.getNodeAddress());
            prevConnection.sendMessage(new KVM(StatusType.MOVE_REPLICA_TO_MAIN_CACHE, "", this.address));
        } catch (Exception e) {
            logger.error(
                    "could not run post closed properly. Couldn't move replica data to main cache on the successor node.",
                    e);
        }

        try {
            this.ecsClient.updateBackupEcsMetadata();
        } catch (Exception e) {
            System.out.println("error updating backup ecs");
        }
        try {
            this.ecsClient.kvMetadata.deleteServer(this.address);
        } catch (Exception e) {
            System.out.println("Connection already closed");
        }
        this.close();
        this.ecsClient.serverConnections.remove(this);
    }

    public void sendUpdateMetadataMessage() throws IOException {
        this.sendMessage(new KVM(StatusType.UPDATE_METADATA, " ", this.ecsClient.kvMetadata.getKeyRange()));
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
            if (status.equals(StatusType.NEW_ECS)) {
                address = value;
                this.ecsClient.addBackupEcsConnection(this);
                sendUpdateMetadataMessage();
                this.ecsClient.sendUpdateAllServerBackupEcsAddresses();
            } else if (status.equals(StatusType.NEW_SERVER_CONNECTING_TO_BACKUP_ECS)) {
                this.ecsClient.addServerConnection(this);
                address = value;
            } else if (status.equals(StatusType.NEW_SERVER)) {
                this.ecsClient.addServerConnection(this);
                address = value;
                this.ecsClient.kvMetadata.addServer(value);
                if (this.ecsClient.backupEcsConnection != null) {
                    this.sendMessage(
                            new KVM(StatusType.SET_BACKUP_ECS_ADDRESS, "", this.ecsClient.backupEcsConnection.address));
                }
                this.sendMessage(new KVM(StatusType.UPDATE_METADATA, " ", this.ecsClient.kvMetadata.getKeyRange()));

                this.sendMessage(new KVM(StatusType.START_SERVER, " ", ""));

                // This should be in the if statement I think

                if (this.ecsClient.kvMetadata.getCountServers() != 1) {
                    ECSNode prevNode = this.ecsClient.kvMetadata.getSuccesorNode(value);
                    logger.info(String.format("Updating prev metadata for %s. The node being added is %s",
                            prevNode.getNodeAddress(), value));
                    ServerConnection prevConnection = this.ecsClient
                            .getServerConnectionWithAddress(prevNode.getNodeAddress());

                    prevConnection.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, " ", " "));
                    Thread.sleep(500);
                    prevConnection.sendMessage(new KVM(StatusType.SEND_FILTERED_DATA_TO_NEXT,
                            this.ecsClient.kvMetadata.getKeyRange(), this.address));
                } else {
                    logger.info("Updating ecs metadata");
                    this.ecsClient.updateBackupEcsMetadata();
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
                this.ecsClient.updateBackupEcsMetadata();
                this.ecsClient.updateAllServerReplicas();
                sendResponse = false;
                Thread.sleep(10);
                this.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, " ", " "));
            } else if (status.equals(StatusType.MOVE_REPLICA_TO_MAIN_CACHE_SUCCESS)
                    || status.equals(StatusType.MOVE_REPLICA_TO_MAIN_CACHE_FAIL)) {
                Thread.sleep(50);
                this.ecsClient.updateAllServerMetadatas();
                this.ecsClient.updateBackupEcsMetadata();
                this.ecsClient.updateAllServerReplicas();
                Thread.sleep(50);
            } else if (status.equals(StatusType.DATA_MOVED_CONFIRMATION_SHUTDOWN)) {
                this.ecsClient.updateAllServerMetadatas();
                this.ecsClient.updateBackupEcsMetadata();
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

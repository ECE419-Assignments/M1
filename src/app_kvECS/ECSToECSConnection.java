package app_kvECS;

import java.io.IOException;

import shared.BaseConnection;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;

public class ECSToECSConnection extends BaseConnection {

    protected ECSClient ecsClient;

    public ECSToECSConnection(ECSClient ecsClient, String address) throws IOException, InterruptedException {
        super(address);
        this.ecsClient = ecsClient;

        Thread.sleep(100);
    }

    @Override()
    public void postStart() throws IOException {
        sendMessage(
                new KVM(StatusType.NEW_ECS, " ", String.format("%s:%s", ecsClient.getHostname(), ecsClient.getPort())));
    }

    @Override()
    public void postClosed() {
        this.ecsClient.switchToPrimeEcs();
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
            if (status.equals(StatusType.UPDATE_METADATA)) {
                this.ecsClient.kvMetadata.createServerTree(value);
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

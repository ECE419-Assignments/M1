package shared.messages;

import java.nio.charset.StandardCharsets;

public class KVM implements KVMessage {

    private String key;
    private String value;
    private StatusType status;

    public KVM(byte[] msgBytes) {
        String decoded = new String(msgBytes, StandardCharsets.US_ASCII);
        String[] parts = decoded.split(":");

        if (!parts[0].matches("[a-zA-Z]+") || !parts[1].matches("[a-zA-Z]+")) {
            throw new IllegalArgumentException("Key and value must only contain characters in the alphabet");
        }

        this.key = parts[0];
        this.value = parts[1];
        this.status = StatusType.valueOf(parts[2]);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }
}
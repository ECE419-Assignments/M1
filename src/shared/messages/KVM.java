package shared.messages;

import java.nio.charset.StandardCharsets;
import java.lang.IllegalArgumentException;

public class KVM implements KVMessage {

    private String key;
    private String value;
    private StatusType status;
    private String msg;
    private byte[] msgBytes;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private static final String PIPE = ":";

    public KVM(byte[] msgBytes) throws Exception {
        this.msgBytes = addCtrChars(msgBytes);
        this.msg = new String(msgBytes, StandardCharsets.US_ASCII);
        System.out.println(this.msg);
        this.splitString(this.msg);
    }

    public KVM(StatusType status, String key, String value) throws IllegalArgumentException {
        if (this.validInputs(status, key, value)) {
            this.key = key;
            this.value = value;
            this.status = status;
            this.msg = createMsgProtocol();
            this.msgBytes = toByteArray(msg);
        } else {
            throw new IllegalArgumentException("Key and Value cannot include special characters.");
        }
    }

    private void splitString(String msg) throws Exception {
        String[] parts = msg.split(":");
        this.status = StatusType.values()[Integer.parseInt(parts[0].trim())];
        this.key = parts[1];
        this.value = parts[2];

    }

    private String createMsgProtocol() {
        StringBuilder tmp = new StringBuilder();
        tmp.append(this.status.ordinal()).append(PIPE).append(this.key).append(PIPE).append(this.value);
        return tmp.toString();
    }

    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    private byte[] toByteArray(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.US_ASCII);
        byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    private boolean validInputs(StatusType status, String key, String value) {
        if (key.contains("\n\r") || key.contains(":")) {
            return false;
        } else if (value.contains("\n\r") || value.contains(":")) {
            return false;
        } else {
            return true;
        }
    }

    public byte[] getMsgBytes() {
        return msgBytes;
    }

    public String getMsg() {
        StringBuilder formatedMsg = new StringBuilder();
        formatedMsg.append(status.toString()).append("//").append(this.key).append(":").append(this.value);
        return formatedMsg.toString();
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.status;
    }
}
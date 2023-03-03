package app_kvServer;

public class KVServerResponse {
    public enum KVServerResponseCode {
        SERVER_NOT_RESPONSIBLE("::SERVER_NOT_RESPONSIBLE"),
        GET_SUCCESS("::GET_SUCCESS"),
        GET_ERROR("::GET_ERROR");

        private final String text;

        KVServerResponseCode(final String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    };

    public KVServerResponseCode ResponseCode;
    public String stringValue;
    public Boolean boolValue;
    public Integer intValue;

    public KVServerResponse(KVServerResponseCode responseCode, String value) {
        this.ResponseCode = responseCode;
        this.stringValue = value;
    }

    public KVServerResponse(KVServerResponseCode responseCode, Boolean value) {
        this.ResponseCode = responseCode;
        this.boolValue = value;
    }

    public KVServerResponse(KVServerResponseCode responseCode, Integer value) {
        this.ResponseCode = responseCode;
        this.intValue = value;
    }
}

package shared.metadata;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;

import shared.ecs.ECSNode;

public class KVMetadata {

    private static String hasher_type = "MD5";
    private MessageDigest hasher;
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    private TreeMap<String, ECSNode> server_tree;

    // Constructor
    public KVMetadata() {
        this.server_tree = new TreeMap<String, ECSNode>();
        try {
            this.hasher = MessageDigest.getInstance(this.hasher_type);
        } catch (NoSuchAlgorithmException e) {
        }
    }

    // Delete a server from the given binary tree
    public ECSNode deleteServer(String server_address) {
        String hex_string = this.hashValue(server_address);
        ECSNode removed_node = this.server_tree.remove(hex_string);

        updateNodeHashRanges();

        return removed_node;
    }

    // Add server to the given binary tree
    public ECSNode addServer(String server_address) {
        String hex_string = this.hashValue(server_address);
        String[] server_info = server_address.split(":");
        ECSNode server_node = new ECSNode(server_info[0], Integer.valueOf(server_info[1]));
        this.server_tree.put(hex_string, server_node);

        updateNodeHashRanges();

        ECSNode added_node = this.getServerNode(server_address);

        return added_node;
    }

    private boolean updateNodeHashRanges() {

        for (Map.Entry<String, ECSNode> entry : this.server_tree.entrySet()) {
            String key = entry.getKey();
            ECSNode server_node = entry.getValue();

            server_node.updateNodeHashRanges(this.getServerHashRange(key));
            this.server_tree.put(key, server_node);
        }
        return true;

    }

    // Returns the node ascoiated with the server info from the server tree
    public ECSNode getServerNode(String server_address) {
        String hex_string = this.hashValue(server_address);
        ECSNode node = this.server_tree.get(hex_string);

        return node;
    }

    // Determines which server the Key is handeled by
    public ECSNode getKeysServer(String key) {
        String hex_string = this.hashValue(key);

        String server_key = this.server_tree.lowerKey(hex_string);
        // If there is no hash lower get the greatest (loop around)
        if (server_key == null) {
            server_key = this.server_tree.lastKey();
        }

        ECSNode node = this.server_tree.get(server_key);
        return node;
    }

    public ECSNode getSuccesorNode(String server_address) {
        ECSNode succesor_node = getKeysServer(server_address);
        return succesor_node;
    }

    // Hashes a value
    public String hashValue(String msg) {
        this.hasher.update(msg.getBytes());
        byte[] digest = this.hasher.digest();
        String hex_number = this.bytesToHex(digest);
        return hex_number;
    }

    // Sourced from
    // https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
    // Converts the bytes to a string of Hex Values
    public String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public String[] getServerHashRange(String key) {
        String[] hash_range = new String[2];

        hash_range[0] = key;

        hash_range[1] = this.server_tree.higherKey(hash_range[0]);

        if (this.server_tree.size() == 0) {
            hash_range[1] = hash_range[0];
        } else if (hash_range[1] == null) {
            hash_range[1] = this.server_tree.firstKey();
        }

        return hash_range;

    }

    // Used for parsing strings received from key range request
    public boolean createServerTree(String key_range) {
        this.server_tree = new TreeMap<String, ECSNode>();

        String[] tokens = key_range.split(";");

        for (String entry : tokens) {
            String[] server_def = entry.split(",");
            String[] server_address = server_def[2].split(":");
            String[] hash_range = { server_def[0], server_def[1] };
            ECSNode server_node = new ECSNode(server_address[0], Integer.valueOf(server_address[1]));
            server_node.updateNodeHashRanges(hash_range);

            this.server_tree.put(server_def[0], server_node);
        }

        return true;
    }

    // Used for creating strings for key range requests
    public String getKeyRange() {

        StringBuilder key_range = new StringBuilder();

        for (Map.Entry<String, ECSNode> entry : this.server_tree.entrySet()) {
            ECSNode value = entry.getValue();

            String[] hash_range = value.getNodeHashRange();
            String host = value.getNodeHost();
            Integer port = value.getNodePort();
            key_range.append(hash_range[0]).append(",").append(hash_range[1]).append(",");
            key_range.append(host).append(":").append(port.toString()).append(";");
        }

        return key_range.toString();
    }

    // testing
    public static void main(String[] args) {

        KVMetadata hasher = new KVMetadata();
        KVMetadata hasher_2 = new KVMetadata();

        for (int i = 0; i < 5; i++) {
            hasher.addServer("localhost:" + 3000 + i);
        }

        for (int i = 0; i < 10; i++) {
            ECSNode that = hasher.getKeysServer(Integer.toString(i * 2000));
            System.out.println(that);
        }

        String key_range = hasher.getKeyRange();
        System.out.println(key_range);

        hasher_2.createServerTree(key_range);
        key_range = hasher_2.getKeyRange();
        System.out.println(key_range);

        for (int i = 0; i < 5; i++) {
            ECSNode node = hasher_2.getServerNode("localhost:" + 3000 + i);
            ECSNode succesor_node = hasher_2.getSuccesorNode("localhost:" + 3000 + i);
            System.out.println(succesor_node);
            System.out.println(node);
        }

    }

}
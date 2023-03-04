package shared;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;

import ecs.ECSNode;

public class KVHasher { // TODO: Zeni - Remove all updateNodeHashRanges from this class.
    // Reasoning: The class is used for hashing, it should not be changing
    // information on ECS nodes. The two classes are too coupled right now

    private static String hasher_type = "MD5";
    private static MessageDigest hasher;
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    // Constructor
    public KVHasher() {
        try {
            this.hasher = MessageDigest.getInstance(this.hasher_type);
        } catch (NoSuchAlgorithmException e) {
        }
    }

    // Delete a server from the given binary tree
    public TreeMap<String, ECSNode> deleteServer(TreeMap<String, ECSNode> server_tree, String server_info) {
        String hex_string = this.hashValue(server_info);
        ECSNode rvalue = server_tree.remove(hex_string);

        server_tree = updateNodeHashRanges(server_tree);

        return server_tree;
    }

    // Add server to the given binary tree
    public TreeMap<String, ECSNode> addServer(TreeMap<String, ECSNode> server_tree, String server_info, ECSNode node) {
        String hex_string = this.hashValue(server_info);
        server_tree.put(hex_string, node);

        server_tree = updateNodeHashRanges(server_tree);

        return server_tree;
    }

    private TreeMap<String, ECSNode> updateNodeHashRanges(TreeMap<String, ECSNode> server_tree) {

        for (Map.Entry<String, ECSNode> entry : server_tree.entrySet()) {
            String key = entry.getKey();
            ECSNode server_node = entry.getValue();

            server_node.updateNodeHashRanges(this.getServerHashRange(server_tree, key));
            server_tree.put(key, server_node);
        }
        return server_tree;

    }

    // Returns the node associated with the server info from the server tree
    public ECSNode getServerNode(TreeMap<String, ECSNode> server_tree, String server_info) {
        String hex_string = this.hashValue(server_info);
        ECSNode node = server_tree.get(hex_string);

        return node;
    }

    // Determines which server the Key is handled by
    public ECSNode getKeysServer(TreeMap<String, ECSNode> server_tree, String key) {
        String hex_string = this.hashValue(key);

        String server_key = server_tree.lowerKey(key);
        // If there is no hash lower get the greatest (loop around)
        if (server_key == null) {
            server_key = server_tree.lastKey();
        }

        ECSNode node = server_tree.get(server_key);
        return node;
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

    public String[] getServerHashRange(TreeMap<String, ECSNode> server_tree, String key) {
        String[] hash_range = new String[2];

        hash_range[0] = key;

        hash_range[1] = server_tree.higherKey(hash_range[0]);

        if (server_tree.size() == 0) {
            hash_range[1] = hash_range[0];
        } else if (hash_range[1] == null) {
            hash_range[1] = server_tree.firstKey();
        }

        return hash_range;

    }

    // Used for parsing strings received from key range request
    public TreeMap<String, ECSNode> createServerTree(String key_range) {
        TreeMap<String, ECSNode> server_tree = new TreeMap<String, ECSNode>();

        String[] tokens = key_range.split(";");

        for (String entry : tokens) {
            String[] server_def = entry.split(",");
            String[] server_info = server_def[2].split(":");
            String[] hash_range = { server_def[0], server_def[1] };
            ECSNode server_node = new ECSNode(server_info[0], Integer.valueOf(server_info[1]));
            server_node.updateNodeHashRanges(hash_range);

            server_tree.put(server_def[0], server_node);
        }

        return server_tree;
    }

    // Used for creating strings for key range requests
    public String getKeyRange(TreeMap<String, ECSNode> server_tree) {

        StringBuilder key_range = new StringBuilder();

        for (Map.Entry<String, ECSNode> entry : server_tree.entrySet()) {
            String key = entry.getKey();
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

        KVHasher hasher = new KVHasher();
        ECSNode node;
        TreeMap<String, ECSNode> server_tree = new TreeMap<String, ECSNode>();

        for (int i = 0; i < 5; i++) {
            node = new ECSNode("localhost", 3000 + i);
            server_tree = hasher.addServer(server_tree, "localhost:" + 3000 + i, node);
        }

        for (int i = 0; i < 10; i++) {
            ECSNode that = hasher.getKeysServer(server_tree, Integer.toString(i * 2000));
        }

        String key_range = hasher.getKeyRange(server_tree);
        System.out.println(key_range);
        TreeMap<String, ECSNode> server_tree_2 = hasher.createServerTree(key_range);
        key_range = hasher.getKeyRange(server_tree_2);
        System.out.println(key_range);
    }

}
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;
import java.nio.charset.StandardCharsets;

import ecs.ECSNode;

public class KVHasher<T> {
  
    private static String hasher_type = "MD5";
    private static MessageDigest hasher;
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    // Constructor
    public KVHasher(){
        try {
            this.hasher = MessageDigest.getInstance(this.hasher_type);
        } catch (NoSuchAlgorithmException e){}
    }

    // Delete a server from the given binary tree
    public TreeMap<String, T> deleteServer(TreeMap<String, T> server_tree, String server_info){
        String hex_string = this.hashValue(server_info);
        T rvalue = server_tree.remove(hex_string);

        return server_tree;
    }

    // Add server to the given binary tree
    public TreeMap<String, T> addServer(TreeMap<String, T> server_tree, String server_info, T node){
        String hex_string = this.hashValue(server_info);
        server_tree.put(hex_string, node);

        return server_tree;
    }

    // Returns the node ascoiated with the server info from the server tree
    public T getServerNode(TreeMap<String, T> server_tree, String server_info){
        String hex_string = this.hashValue(server_info);
        T node = server_tree.get(hex_string);

        return node;
    }

    // Determines which server the Key is handeled by
    public T getKeysServer(TreeMap<String, T> server_tree, String key){
        // What if return null
        String hex_string = this.hashValue(key);
        String server_key = server_tree.lowerKey(key);

        // If there is no hash lower get the greatest (loop around)
        if (server_key == null){
            server_key = server_tree.lastKey();
        }

        T node = server_tree.get(server_key);
        return node;
    }

    // Hashes a value
    public String hashValue (String msg){
        this.hasher.update(msg.getBytes());
        byte[] digest = this.hasher.digest();
        String hex_number = this.bytesToHex(digest);
        return hex_number;
    }

    // Sourced from https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
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

    // Used for parsing strings received from key range request
    public TreeMap<String,String[]> createServerTree(String key_range){
        return null;
    }

    // Used for creating strings for key range requests
    public String createKeyRange(TreeMap<String,T> server_tree){
        return null;
    }

    // testing
    public static void main(String[] args){

        KVHasher<ECSNode> hasher = new KVHasher<ECSNode>();
        ECSNode node;
        TreeMap<String, ECSNode> server_tree = new TreeMap<String, ECSNode>();

        for (int i=0; i<5; i++){
            node = new ECSNode();
            server_tree = hasher.addServer(server_tree, "localhost:"+3000+i, node);
        }
            System.out.println(server_tree.size());

        for (int i=0; i<10; i++){
            ECSNode that = hasher.getKeysServer(server_tree, Integer.toString(i*2000));
            System.out.println(that);
        }

    }

}
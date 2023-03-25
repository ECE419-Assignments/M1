package shared.ecs;

public class ECSNode implements IECSNode {

    String host;
    int port;
    String[] hash_range;

    public ECSNode(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getNodeAddress() {
        return String.format("%s:%s", host, port);
    }

    @Override
    public String getNodeName() {
        return null;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String[] getNodeHashRange() {
        return hash_range;
    }

    public void updateNodeHashRanges(String[] hash_range) {
        this.hash_range = hash_range;
    }

}

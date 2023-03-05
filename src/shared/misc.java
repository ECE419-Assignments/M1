package shared;

public class misc {
    public static String getHostFromAddress(String address) {
        return address.split(":")[0];
    }

    public static int getPortFromAddress(String address) {
        return Integer.parseInt(address.split(":")[1]);
    }
}

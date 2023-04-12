package testing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.TestCase;

public class ECSBackupTest extends TestCase {
    ECSClient ecs1, ecs2;
    KVServer server1, server2;

    public void setUp() {
        ecs1 = new ECSClient(51001, "", false);
        server1 = new KVServer(7000, 12, CacheStrategy.FIFO, "127.0.0.1", 51001);
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            System.out.println("error");
        }
        ecs2 = new ECSClient(51002, "127.0.0.1:51001", true);
    }

    public void tearDown() {
        try {
            ecs1.close();
        } catch (Exception e) {

        }
        try {
            ecs2.close();
        } catch (Exception e) {

        }
        try {
            server1.close();
        } catch (Exception e) {

        }
        try {
            server2.close();
        } catch (Exception e) {

        }
    }

    @Test()
    public void testSetServerBackupAddress() {
        assertEquals(server1.backupEcsAddress, "127.0.0.1:51002");
    }

    @Test()
    public void testSetServerKeyrange() {
        assertNotNull(server1.metadata.getKeyRange());
    }

    @Test()
    public void testServerConnectBackupEcs() {
        try {
            server1.putKV("hi", "hello");
            ecs1.close();
            Thread.sleep(1000);
            String value = server1.getKV("hi");
            assertEquals(value, "hello");
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("error");
            assertEquals(false, true);
        }
    }

    @Test()
    public void testEcsDeleteKeyrange() {
        try {
            String keyrange = server1.metadata.getKeyRange();
            ecs1.close();
            Thread.sleep(1000);
            String keyrange2 = server1.metadata.getKeyRange();
            assertEquals(keyrange, keyrange2);
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("error");
            assertEquals(false, true);
        }
    }

    @Test()
    public void testEcsDeleteServerAdd() {
        try {
            server1.putKV("hi", "hello");
            ecs1.close();
            Thread.sleep(1000);
            KVServer server2 = new KVServer(7005, 12, CacheStrategy.FIFO, "127.0.0.1", 51002);
            server2.putKV("hi", "hello");
            String value = server2.getKV("hi");
            assertEquals(value, "hello");
            server2.close();
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("error");
            assertEquals(false, true);
        }
    }

    @Test()
    public void testEcsDeleteServerAddKeyrange() {
        try {
            server1.putKV("hi", "hello");
            ecs1.close();
            Thread.sleep(1000);
            KVServer server2 = new KVServer(7005, 12, CacheStrategy.FIFO, "127.0.0.1", 51002);
            Thread.sleep(1000);
            assertEquals(server2.metadata.getKeyRange(), server1.metadata.getKeyRange());
            server2.close();
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("error");
            assertEquals(false, true);
        }
    }

    @Test()
    public void testEcsDeleteServerDelete() {
        try {
            server1.putKV("hi", "hello");
            ecs1.close();
            Thread.sleep(500);
            ECSClient ecs3 = new ECSClient(51002, "127.0.0.1:51002", true);
            Thread.sleep(1000);
            KVServer server2 = new KVServer(7005, 12, CacheStrategy.FIFO, "127.0.0.1", 51002);
            server2.putKV("hi", "hello");
            String value = server2.getKV("hi");
            assertEquals(value, "hello");
            server2.deleteKV("hi", false);
            server2.close();
            ecs3.close();
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("error");
            assertEquals(false, true);
        }
    }

    @Test()
    public void testEcsDeleteServerDeleteKeyrange() {
        try {
            String keyrange = server1.metadata.getKeyRange();
            ecs1.close();
            Thread.sleep(500);
            ECSClient ecs3 = new ECSClient(51002, "127.0.0.1:51002", true);
            Thread.sleep(1000);
            KVServer server2 = new KVServer(7005, 12, CacheStrategy.FIFO, "127.0.0.1", 51002);
            server2.putKV("hi", "hello");
            String value = server2.getKV("hi");
            assertEquals(keyrange, server2.metadata.getKeyRange());
            server2.deleteKV("hi", false);
            server2.close();
            ecs3.close();
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("error");
            assertEquals(false, true);
        }
    }

    @Test()
    public void testEcsDeleteBackupServerAddress() {
        try {
            server1.putKV("hi", "hello");
            ecs1.close();
            Thread.sleep(500);
            ECSClient ecs3 = new ECSClient(51003, "127.0.0.1:51002", true);
            Thread.sleep(1000);
            String backupAddress = server1.backupEcsAddress;
            ecs3.close();
            Thread.sleep(1000);
            assertEquals(backupAddress, "127.0.0.1:51003");
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("error");
            assertEquals(false, true);
        }
    }

    @Test()
    public void testEcsDeleteBackupServerAddressKeyrange() {
        try {
            String keyrange = server1.metadata.getKeyRange();
            ecs1.close();
            Thread.sleep(500);
            ECSClient ecs3 = new ECSClient(51003, "127.0.0.1:51002", true);
            Thread.sleep(1000);
            ecs3.close();
            Thread.sleep(1000);
            assertEquals(keyrange, server1.metadata.getKeyRange());
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("error");
            assertEquals(false, true);
        }
    }

}

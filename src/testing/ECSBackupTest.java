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
    public void testEcsDeleteServerAdd() {
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
    public void testEcsDeleteServerDelete() {
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
    public void testDoubleEcsDelete() {
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
}

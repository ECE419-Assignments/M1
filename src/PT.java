import java.io.IOException;

import org.apache.log4j.Level;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import app_kvServer.IKVServer.CacheStrategy;
import logger.LogSetup;
import shared.messages.KVM;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileNotFoundException;

import java.util.Random;
import java.util.ArrayList;

public class PT implements Runnable {

    private KVStore kvClient;
    private int base_port;
    public double put_time = 0;
    public double get_time = 0;
    public boolean running = true;

    public PT(int base_port) {
        this.base_port = base_port;
    }

    public void run() {
        KVM msg = null;
        double startTime = System.currentTimeMillis();
        double endTime = System.currentTimeMillis();
        Random rand = new Random();
        try {
            this.kvClient = new KVStore("localhost", this.base_port);
            this.kvClient.connect();
        } catch (Exception e) {
            System.out.println(e);
        }

        for (int i = 0; i < 50; i++) {
            try {
                String key = "key" + rand.nextInt(50);
                startTime = System.currentTimeMillis();
                kvClient.put(key, "vlad");
                endTime = System.currentTimeMillis();
                this.put_time += (endTime - startTime); //* (1.0 / 50.0);

                key = "key" + rand.nextInt(50);
                startTime = System.currentTimeMillis();
                kvClient.get(key);
                endTime = System.currentTimeMillis();
                this.get_time += (endTime - startTime); //* (1.0 / 50.0);

            } catch (Exception e) {
                System.out.println(e);
            }
        }
        this.get_time = this.get_time / 50.0;
        this.put_time = this.put_time / 50.0;
        this.running = false;
    }

    public static void main(String[] args) {
        int nmb_servers = Integer.parseInt(args[0]);
        int nmb_clients = Integer.parseInt(args[1]);
        int cacheSize = Integer.parseInt(args[2]);
        int base_port = Integer.parseInt(args[3]);

        try {
            new ECSClient(51001, "", false);
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            new ECSClient(51002, "51001", true);;
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        

        // Create servers
        for (int i = 0; i < nmb_servers; i++) {
            new KVServer(base_port + i, cacheSize, CacheStrategy.FIFO, "localhost", 51001);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Run client threads
        ArrayList<PT> clients = new ArrayList<PT>();
        for (int i = 0; i < nmb_servers; i++) {
            PT client_run = new PT(base_port);
            clients.add(client_run);
            Thread thread = new Thread(client_run);
            thread.start();
        }
        float avg_get = 0;
        float avg_put = 0;
        for (PT client : clients) {
            while (client.running) {
                avg_get += client.get_time * (1.0 / Double.valueOf(nmb_clients));
                avg_put += client.put_time * (1.0 / Double.valueOf(nmb_clients));
            }
        }
        try {
            String data1 = "Average Get:" + avg_get;
            String data2 = "Average Put:" + avg_put;
            FileWriter writer = new FileWriter("out.txt");
            writer.write(data1);
            writer.write("\n");
            writer.write(data2);
            writer.flush();
        } catch (IOException e) {
            System.out.println(e);
        }
        System.out.println("Average Get:" + avg_get);
        System.out.println("Average Put:" + avg_put);
    }
}

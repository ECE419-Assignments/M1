package app_kvServer;
import java.io.IOException;

public class KVServerHook extends Thread{

    ECSConnection connection;

    public KVServerHook(ECSConnection connection){
        this.connection = connection;
    }
    public void run(){
        System.out.println("Shutdown Hook is running !");

        try{
		    this.connection.serverShuttingDown();
        }catch (IOException e) {
            System.out.println("Graceful exit failed");
        }
    }
}

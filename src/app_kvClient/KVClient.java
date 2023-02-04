package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVServer> ";
	private BufferedReader stdin;
	private boolean stop = false;
	private String hostname;
	private int port;
    private KVStore kvStore = null;

    @Override
    public void newConnection(String hostname, int port) throws Exception{

    }

    @Override
    public KVCommInterface getStore(){
        KVCommInterface test = null;
        return test;
    }

    public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

    private void handleCommand(String cmdLine){
        String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					hostname = tokens[1];
					port = Integer.parseInt(tokens[2]);
					System.out.println(hostname + port);
					kvStore = newConnection(hostname, port);
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}
        }else if (tokens[0].equals("put")){
            if(tokens.length == 3) {
                if(kvStore != null && kvStore.isRunning()){
                    String key = tokens[1];
                    String value = tokens[2];
                    try {
                        //TODO Message handeling from kVstore
                        kvStore.put(key, value);
                    } catch (IOException e) {
                        printError("Unable to send message!");
                        disconnect();
                    }     
                }
				
			} else {
				printError("Invalid number of parameters!");
			}
        }else if (tokens[0].equals("get")){
            if(tokens.length == 2) {
                if(kvStore != null && kvStore.isRunning()){
                    String key = tokens[1];
                    String value = tokens[2];
                    try {
                        //TODO Message handeling from kVstore
                        kvStore.get(key);
                    } catch (IOException e) {
                        printError("Unable to send message!");
                        disconnect();
                    }
                }
				
			} else {
				printError("Invalid number of parameters!");
			}
        } else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
        
        } else if(tokens[0].equals("disconnect")) {
			disconnect();
        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
		}
    }

    private void disconnect() {
		if(kvStore != null) {
			kvStore.disconnect();
			kvStore = null;
		}
	}
    //TODO: Add get and put
    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("send <text message>");
		sb.append("\t\t sends a text message to the server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

    private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

    /*
    Entry point for the client side.
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.OFF);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }

}

/*
 * TODO: This is the application interface.
 * Anything received from the CLI should essentially just call KVStore to run some shit.
 */

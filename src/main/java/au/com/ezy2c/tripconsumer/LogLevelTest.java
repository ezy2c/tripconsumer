package au.com.ezy2c.tripconsumer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class LogLevelTest {
    public static void main(String[] arg) throws IOException {
    	Logger log = Logger.getLogger(LogLevelTest.class.getName());
        log.setLevel(Level.ALL);
    	log.info("initializing - trying to load configuration file ...");

    	//Properties preferences = new Properties();
    	try {
    	    //FileInputStream configFile = new FileInputStream("logging.properties");
    	    //preferences.load(configFile);
    	    LogManager.getLogManager().readConfiguration(LogLevelTest.class.getClassLoader().getResourceAsStream("logging.properties"));
    	} catch (IOException ex)
    	{
    	    System.out.println("WARNING: Could not open configuration file");
    	    System.out.println("WARNING: Logging not configured (console output only)");
    	}
    	log.info("starting myApp");
    	log.warning("warning");
    	log.fine("fine");
    	log.severe("severe");
    }
}
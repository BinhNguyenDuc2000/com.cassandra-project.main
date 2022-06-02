package log;

import java.io.File;
import java.sql.Timestamp;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
/**
 * Logging running processes.
 * @author Binh.NguyenDuc2000@gmail.com
 *
 */
public class Log {
	private Logger logger;
	private FileHandler fileHandler;
	private SimpleFormatter formatter;
	
	/**
	 * Initializing logging file.
	 * 
	 * Log file name is decided by current time.
	 */
	public Log() {
		try {
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			this.logger = Logger.getLogger("FormatInputController");
			String fileName = new String("log-" + timestamp.getTime() + ".txt");
			File file = new File(fileName);
			fileHandler = new FileHandler(file.getName(), true);
			logger.addHandler(fileHandler);
			formatter = new SimpleFormatter();
			fileHandler.setFormatter(formatter);
			logger.setLevel(Level.INFO);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void startLogger() {
		logger.info("Starting");
	}
	
	public void endLogger() {
		logger.info("Stopping");
	}
	
	/**
	 * Logging a specific message.
	 * @param message The message to be logged.
	 */
	public void info(String message) {
		logger.info(message);
	}
}

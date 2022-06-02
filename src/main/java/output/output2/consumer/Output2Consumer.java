package output.output2.consumer;

import java.io.BufferedWriter;
import java.util.concurrent.BlockingQueue;

/**
 * Consumer will take records from Blocking Queue, standardize the Owner name and print it to final Output file.
 * @author Binh.NguyenDuc2000@gmail.com
 *
 */
public class Output2Consumer implements Runnable{
	private BlockingQueue<String> dataQueue;
	private BufferedWriter writer;

	public Output2Consumer(BlockingQueue<String> dataQueue, BufferedWriter writer) {
		this.dataQueue = dataQueue;
		this.writer = writer;
	}

	@Override
	public void run() {
		consume();
	}

	private void consume() {
		try {
			String message;
			boolean running = true;
			while (running) {
				message = dataQueue.take();
				if (message.compareTo("end") != 0) {
					writer.write(message + "\n");
				} else {
					running = false;
				}
			}
			writer.write("####\n");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
}

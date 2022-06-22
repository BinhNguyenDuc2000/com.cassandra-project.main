package output.output1.consumer;

import java.io.BufferedWriter;
import java.util.concurrent.BlockingQueue;
/**
 * Consumer will take records from Blocking Queue and print it to final Output file.
 * @author Binh.NguyenDuc2000@gmail.com
 *
 */
public class Output1Consumer implements Runnable {
	private BlockingQueue<String> dataQueue;
	private BufferedWriter writer;

	public Output1Consumer(BlockingQueue<String> dataQueueList, BufferedWriter writer) {
		this.dataQueue = dataQueueList;
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
					writer.write(message);
				} else {
					running = false;
				}
			}

			writer.write("####\n");

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}

package input.consumer;

import java.util.concurrent.BlockingQueue;

import datastax.core.Core;

/**
 * Taking records data from Blocking queue to print to Middle Output file.
 * @author Binh.NguyenDuc2000@gmail.com
 *
 */
public class InputConsumer implements Runnable {
	private BlockingQueue<String> dataQueue;
	private Core core;

	public InputConsumer(BlockingQueue<String> dataQueue, Core core) {
		this.dataQueue = dataQueue;
		this.core = core;
	}

	@Override
	public void run() {
		consume();
	}

	/**
	 * Continue to read from blocking queue, stopping when 'end' message is received.
	 */
	private void consume() {
		try {
			while (true) {
				String message;
				message = dataQueue.take();
				if (message.compareTo("end") != 0) {
					int strLen = message.length();
					int index = message.charAt(strLen-1) - 48;
					if (message.charAt(strLen-2)!=',') {
						index += (message.charAt(strLen-2) - 48) * 10;
					}
					core.insertDevice(message, index);
				}
				else {
					dataQueue.put("end");
					break;
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
	}
}

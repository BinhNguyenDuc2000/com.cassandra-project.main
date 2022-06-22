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
	private int warrantyYear;
	private static final int BATCH_SIZE = 50;
	private static final int COMPRESS_SIZE = 1500; // <= 1000 * ~ 65 == Cassandra limit 

	public InputConsumer(BlockingQueue<String> dataQueue, Core core, int warrantyYear) {
		this.dataQueue = dataQueue;
		this.core = core;
		this.warrantyYear = warrantyYear;
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
			String[] dataArray = new String[BATCH_SIZE];
			StringBuilder stringBuilder = new StringBuilder();
			int length = 0;
			int compressed_devices = 0;
			while (true) {
				String message = dataQueue.take();
				if (message.compareTo("end") != 0) {
					stringBuilder.append(message.concat("\n"));
					compressed_devices ++;
					if (compressed_devices >= COMPRESS_SIZE)
					{
						dataArray[length] = compressor.Compressor.Compress(stringBuilder.toString());
						stringBuilder = new StringBuilder();
						compressed_devices = 0;
						
						length++;
						if (length==BATCH_SIZE) {	
							core.insertDevice(dataArray,  warrantyYear, length);
							length = 0;
						}
					}
				}
				else {
					if (compressed_devices>0) {
						dataArray[length] = compressor.Compressor.Compress(stringBuilder.toString());
						length++;
					}
					if (length > 0) {
						core.insertDevice(dataArray, warrantyYear, length);
						length = 0;
					}
					break;
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
	}
}

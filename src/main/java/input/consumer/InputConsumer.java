package input.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

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
	private static final int BATCH_SIZE = 1000;
	private static final int MAX_INNER_THREAD = 1;

	public InputConsumer(BlockingQueue<String> dataQueue, Core core, int warrantyYear) {
		this.dataQueue = dataQueue;
		this.core = core;
		this.warrantyYear = warrantyYear;
	}

	@Override
	public void run() {
		consume();
	}
	
	public void awaitCompletion(List<CompletableFuture<AsyncResultSet>> listCompletableFutures) {
		try {
			for (CompletableFuture<AsyncResultSet> completableFuture: listCompletableFutures) {
				
					completableFuture.get();
				
			}
			listCompletableFutures.clear();
			Thread.sleep(1000);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Continue to read from blocking queue, stopping when 'end' message is received.
	 */
	private void consume() {
		try {
			String[] dataArray = new String[BATCH_SIZE];
			List<CompletableFuture<AsyncResultSet>> listCompletableFutures = new ArrayList<>();
			int length = 0;
			while (true) {
				String message = dataQueue.take();
				if (message.compareTo("end") != 0) {
					dataArray[length] = message;
					length++;
					if (length==BATCH_SIZE) {
						listCompletableFutures.add(core.insertDevice(dataArray.clone(), warrantyYear, length).toCompletableFuture());
						length = 0;
					}
					if (listCompletableFutures.size() >= MAX_INNER_THREAD) { 
						awaitCompletion(listCompletableFutures);
					}
					
				}
				else {
					if (length > 0) {
						listCompletableFutures.add(core.insertDevice(dataArray.clone(), warrantyYear, length).toCompletableFuture());
						length = 0;
					}
					break;
				}
			}
			awaitCompletion(listCompletableFutures);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
	}
}

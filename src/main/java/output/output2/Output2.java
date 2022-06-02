package output.output2;

import java.io.BufferedWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import datastax.core.Core;
import output.output2.consumer.Output2Consumer;
import output.output2.middle_queue.MiddleQueue;
import output.output2.producer.Output2Producer;
/**
 * Reading from Middle Output Files, standardizing owner name and printing it to the final Output file.
 * @author Binh.NguyenDuc2000@gmail.com
 *
 */
public class Output2 {
	private BufferedWriter writer;
	private int range;
	private Core core;
	private static final int NUMBER_OF_MIDDLE_QUEUE_THREAD = 3;
	private static final int NUMBER_OF_MIDDLE_QUEUE_STRINGS = 100000;
	private static final int NUMBER_OF_FINAL_QUEUE_STRINGS = 1000;

	/**
	 * Initializing the writer and range.
	 * @param writer the writer used to print to final Output file.
	 * @param range the range of warranty year.
	 */
	public Output2(BufferedWriter writer, int range, Core core) {
		try {
			this.writer = writer;
			this.range = range;
			this.core = core;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	/**
	 * Executing printing task 2 to file.
	 * 
	 * The process is split into 2 threads: Consumer and Producer. 
	 * - Producer will read records from Middle Output files and add it to the Blocking Queue.
	 * - Consumer will take records from Blocking Queue, standardize the Owner name and print it to final Output file.
	 */
	public void printTask2() {
		try {
			BlockingQueue<String> middleQueue = new ArrayBlockingQueue<String>(NUMBER_OF_MIDDLE_QUEUE_STRINGS);
			BlockingQueue<String> finalQueue = new ArrayBlockingQueue<String>(NUMBER_OF_FINAL_QUEUE_STRINGS);

			ExecutorService executorService = Executors.newFixedThreadPool(1+NUMBER_OF_MIDDLE_QUEUE_THREAD);
			executorService.execute(new Output2Producer(middleQueue, range, core));
			for (int i=0; i<NUMBER_OF_MIDDLE_QUEUE_THREAD; i++) {
				executorService.execute(new MiddleQueue(middleQueue, finalQueue));
			}
			ExecutorService consumerExecutorService = Executors.newFixedThreadPool(1);
			consumerExecutorService.execute(new Output2Consumer(finalQueue, writer));
			consumerExecutorService.shutdown();
			
			consumerExecutorService.shutdown();
			executorService.shutdown();
			while (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES));
			finalQueue.add("end");
			
			while (!consumerExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 

	}
}

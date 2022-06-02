package output.output2.producer;

import java.util.concurrent.BlockingQueue;

import com.datastax.oss.driver.api.core.cql.ResultSet;

import datastax.core.Core;
/**
 * Producer will read records from Middle Output files and add it to the Blocking Queue.
 * @author Binh.NguyenDuc2000@gmail.com
 *
 */
public class Output2Producer implements Runnable{
	private BlockingQueue<String> dataQueue;    
    private int range;
    private Core core;
    public Output2Producer(BlockingQueue<String> dataQueue, int range, Core core) {
        this.dataQueue = dataQueue;
        this.range = range;
        this.core = core;
    }

    @Override
    public void run() {
        produce();
    }
    
    private void produce() {
    	String message;
		for (int i = range-1; i >= 0; i--) {
			try {
				boolean running = true;
				ResultSet resultSet = core.getData(i);
				while (running) {
					message = Core.getNextData(resultSet);
					if (message != null) {
						dataQueue.put(message);
					} else {
						running = false;
					}
				}
			} catch (Exception e1) {
				e1.printStackTrace();
				System.exit(1);
			}
		}
		
		try {
			dataQueue.put("end");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
    }
}

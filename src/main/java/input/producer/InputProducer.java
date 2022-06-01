package input.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import input.Input;

/**
 * The Input file reading thread and adding it to the Blocking Queues List.
 * @author Binh.NguyenDuc2000@gmail.com
 *
 */
public class InputProducer implements Runnable {
    private ArrayList<BlockingQueue<String>> dataQueueList;    
    private BufferedReader reader;
    public InputProducer(ArrayList<BlockingQueue<String>> dataQueueList, BufferedReader reader) {
        this.dataQueueList = dataQueueList;
        this.reader = reader;
    }

    @Override
    public void run() {
        produce();
    }
    
    /**
     * Read through the input file and add records to corresponding Blocking Queue
     * 
     * The thread stops when reads to end of file or enough bytes are read and the reader is close.
     */
    private void produce() {
    	long readChars = 0;
        for ( ;readChars<Input.NUMBER_OF_CHARS_PER_THREAD; ) {
            String message;
			try {
				message = reader.readLine();
				if (message != null) {
//					int strLen = message.length();
//					int index = message.charAt(strLen-1) - 48;
//					if (message.charAt(strLen-2)!=',') {
//						index += (message.charAt(strLen-2) - 48) * 10;
//					}
					readChars += message.length() + 2;
					dataQueueList.get(0).put(message);
				}
				
				else {
					break;
				}
				
			} catch (Exception e1) {
				e1.printStackTrace();
				System.exit(1);
			}  	
        }
        
        try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
        
        
       
    }
    
}
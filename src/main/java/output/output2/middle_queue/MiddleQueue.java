package output.output2.middle_queue;

import java.util.concurrent.BlockingQueue;

public class MiddleQueue implements Runnable {

	private BlockingQueue<String> inputQueue;
	private BlockingQueue<String> outputQueue;

	public MiddleQueue(BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue) {
		this.inputQueue = inputQueue;
		this.outputQueue = outputQueue;
	}

	@Override
	public void run() {
		try {
			String message = inputQueue.take();
			while (message != "end") {
				String parts[] = message.split(",");
				parts[2] = standardizedOwner(parts[2]);
				message = String.join(",", parts);
				outputQueue.put(message);
				message = inputQueue.take();
			}
			inputQueue.put("end");
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public String standardizedOwner(String owner) {
		owner = owner.trim();

		StringBuilder temp = new StringBuilder(String.valueOf(Character.toUpperCase(owner.charAt(0))));
		for (int i = 1; i < owner.length(); i++) {
			if (owner.charAt(i - 1) == ' ') {
				if (Character.isAlphabetic(owner.charAt(i)))
					temp.append(Character.toUpperCase(owner.charAt(i)));
			} else {
				if (Character.isAlphabetic(owner.charAt(i)))
					temp.append(Character.toLowerCase(owner.charAt(i)));
				else {
					temp.append(' ');
				}
			}
		}
		return temp.toString();
	}

}

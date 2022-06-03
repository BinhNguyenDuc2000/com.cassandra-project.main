package faker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Fake data needed to run
 * @author ThinkPad
 *
 */

public class FakeInputData {

	static final String AB = "        ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static final String TRUE_AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	private BufferedWriter writer;
	private DateFormat dateFormat;

	private Random random;

	private int huaweiCounter;
	private int mscCounter;
	private int epcCounter;
	
	/**
	 * Initializing FakeInputData instance.
	 * @param filename the input file name.
	 */
	public FakeInputData(String filename) {
		try {
			writer = new BufferedWriter(new FileWriter(filename));
			huaweiCounter = 0;
			mscCounter = 0;
			epcCounter = 0;
			random = new Random();
			dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}
	
	/**
	 * Add a new line to input file.
	 */
	public void appendLine() {
		try {
			String newline = randomCodeAndName()+ "," + randomOwner() + "," + randomDate() + "," + randomWarrantyYear();
			writer.write(newline);
			writer.newLine();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public String randomOwner() {
		int len = Math.abs(random.nextInt() % 10) + 5;
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(AB.charAt(random.nextInt(AB.length())));
		// at least 1 letter
		sb.insert(random.nextInt(len), TRUE_AB.charAt(random.nextInt(TRUE_AB.length())));
		return sb.toString();
	}

	public String randomCodeAndName() {
		String line = new String();
		switch (Math.abs(random.nextInt() % 3)) {
		case 1:
			line += "HUA" + String.valueOf(huaweiCounter) + "," + "OCS HUAWEI";
			huaweiCounter++;
			break;
		case 2:
			line += "ERICVTTEK" + String.valueOf(mscCounter) + "," + "MSC ERICSSONI";
			mscCounter++;
			break;
		default:
			line += "NSNVTTEK" + String.valueOf(epcCounter) + "," + "EPC ERICSSON";
			epcCounter++;
			break;
		}
		return line;
	}
	
	public String randomDate() {
	    long randomMillisSinceEpoch = ThreadLocalRandom
	      .current()
	      .nextLong(1266395614000l, 1581928414000l);
	    Date date = new Date(randomMillisSinceEpoch);
	    return dateFormat.format(date);
	}
	
	public String randomWarrantyYear() {
		return String.valueOf(Math.abs(random.nextInt())%100);
	}
	
	public void close() {
		try {
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

	public static void main(String[] args) {
		FakeInputData faker = new FakeInputData("Input/input1.txt");
		System.out.print("Number of devices: ");
		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));		
        try {
        	String s = bufferRead.readLine();
			
			for (int i=0; i<Integer.parseInt(s); i++)
				faker.appendLine();
			faker.close();
			
			System.out.print("Finished Faking Input file\n");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

}

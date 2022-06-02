package controller;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;

import javax.swing.JOptionPane;

import datastax.core.Core;
import input.Input;
import log.Log;
import output.output1.Output1;
import output.output2.Output2;

/**
 * Controlling the flow of tasks.
 * @author Binh.NguyenDuc2000@gmail.com
 *
 */

public class FormatInputController {

	private static final Log log = new Log();
	private int range;
	private Timestamp startTime;
	private Timestamp stopTime;
	private Input input;
	private BufferedWriter writer;
	private static final int WRITER_BUFFER_SIZE = 8192 * 10;
	private Output1 output1;
	private Output2 output2;
	private Core core;
	
	/**
	 * Initializing Input and Writer.
	 * @param inputFilename the input file name.
	 * @param outputFileName the output file name.
	 * @param range the range of warranty year.
	 */
	public FormatInputController(String inputFilename, String outputFileName, int range, Core core) {
		this.core = core;
		startTask("Truncating data");
		core.clearAllDevices();
		endTask("Truncating data");
		startTask("Initializing input(Getting file length and setting up readers)");
		this.range = range;
		input = new Input(inputFilename, range, core);
		endTask("Initializing input(Getting file length and setting up readers)");
		try {
			this.writer = new BufferedWriter(new FileWriter(outputFileName), WRITER_BUFFER_SIZE);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
	/**
	 * Starting the logger.
	 */
	public void start() {
		log.startLogger();
		log.info("Size of input:" + JOptionPane.showInputDialog("Input size:"));
		String message = JOptionPane.showInputDialog("Log notes:");
		log.info(message);
	}
	
	/**
	 * Ending the logger and writer.
	 */
	public void end() {
		log.endLogger();
		try {
			writer.close();
			core.endSession();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	/**
	 * Starting a task with a message and a timer.
	 * @param message task description.
	 */
	public void startTask(String message) {
		startTime = new Timestamp(System.currentTimeMillis());
		log.info("Starting task " + message);
	}
	
	/**
	 * Ending a task with a message and total time took.
	 * @param message task description.
	 */
	
	public void endTask(String message) {
		log.info("Ending task "+ message);
		stopTime = new Timestamp(System.currentTimeMillis());
		float runtime = ((float)(stopTime.getTime()-startTime.getTime())/1000);
		log.info("Task " + message + " took " + runtime + "s");
	}
	
	/**
	 * Executing task 1 which involves reading input file and printing sorter records list.
	 */
	public void task1() {	
		
		// Reading input file
		startTask("Reading input file");
		input.readAll();
		endTask("Reading input file");
		
		startTask("Printing task 1");
		output1 = new Output1(writer, range, core);
		output1.printTask1();
		endTask("Printing task 1");
	}
	
	/**
	 * Starting task 2 which involves standardizing records owner name and print the reverse records list.
	 */
	public void task2() {
		
		startTask("Printing to file task2");
		output2 = new Output2(writer, range, core);
		output2.printTask2();
		endTask("Printing to file task2");
	}

	public static void main(String[] args) {
		try {
			Core core = new Core("34.143.164.127", "datacenter1");
			FormatInputController controller = new FormatInputController("Input/input1.txt", "Output/output1.txt", 100, core);
			controller.start();
			controller.task1();
			controller.task2();
			controller.end();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}

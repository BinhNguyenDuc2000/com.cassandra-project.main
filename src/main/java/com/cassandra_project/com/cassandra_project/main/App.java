package com.cassandra_project.com.cassandra_project.main;

import datastax.core.Core;
import input.Input;

public class App 
{
    public static void main( String[] args )
    {
    	Core core = new Core("34.143.164.127", "datacenter1");
		core.clearAllDevices();
		Input input = new Input("Input/input1.txt", 1, core);
		input.readAll();
		
		core.endSession();
    }
}

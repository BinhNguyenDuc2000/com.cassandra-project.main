package datastax.core;
import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import input.Input;


public class Core {
	private CqlSession session;
	public Core(String socketAddress, String dataCenter) {
		try {
			session = CqlSession.builder().
					addContactPoint(new InetSocketAddress(socketAddress, 9042)).
					withLocalDatacenter(dataCenter).
					withKeyspace("binhnguyen").
					build();
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public String getData() {
		ResultSet resultSet = session.execute("select * from devices;");
		StringBuilder result= new StringBuilder();
		for (Row row : resultSet) {
			result.append(row.getString("data"));
			result.append("\n");
		}
		return result.toString();
	}
	
	public void insertDevice(String data, int warranyYear) {
		session.execute(String.format("insert into devices (data, warrantyyear) values "
				+ "(\'%s\' , %d) ;", data, warranyYear));
	}
	
	public void clearAllDevices() {
		session.execute("truncate devices;");
	}
	
	public void endSession() {
		session.close();
	}
	
	public static void main(String[] args) {
		Core core = new Core("34.143.164.127", "datacenter1");
		core.clearAllDevices();
		Input input = new Input("Input/input1.txt", 1, core);
		input.readAll();
		
		System.out.print(core.getData());
		core.endSession();
	}
	
}

package datastax.core;
import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;


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
		String result= "";
		for (Row row : resultSet) {
			result += row.getString("data");
			result += Integer.toString(row.getInt("warrantyyear"));
		}
		return result;
	}
	
	public void insertDevice(String data, String warranyYear) {
		session.execute(String.format("insert into devices (data, warrantyyear) values "
				+ "(%s , %s) ;", data, warranyYear));
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
		core.insertDevice("\'ERICVTTEK01,MSC ERICSSON,nGuyen vAn C,29/12/2016, \'", "5");
		System.out.print(core.getData());
		core.endSession();
	}
	
}

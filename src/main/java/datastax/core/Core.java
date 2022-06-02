package datastax.core;
import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;


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
	
	public ResultSet getData(int warrantyYear) {
		ResultSet resultSet = session.execute(String.format("SELECT * FROM devices WHERE warrantyyear = %d;", warrantyYear));
//		StringBuilder result= new StringBuilder();
//		for (Row row : resultSet) {
//			result.append(row.getString("data"));
//			result.append("\n");
//		}
		return resultSet;
	}
	
	public void insertDevice(String data, int warranyYear) {
		session.execute(String.format("INSERT INTO devices (data, warrantyyear) VALUES "
				+ "(\'%s\' , %d) ;", data, warranyYear));
	}
	
	public void clearAllDevices() {
		session.execute("Truncate devices;");
	}
	
	public void endSession() {
		session.close();
	}
	
	public static String getNextData(ResultSet resultSet) {
		Row row = resultSet.one();
		if (row == null) {
			return null;
		}
		return row.getString("data");
	}
	
}

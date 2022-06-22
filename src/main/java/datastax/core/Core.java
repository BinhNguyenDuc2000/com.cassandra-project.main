package datastax.core;
import java.net.InetSocketAddress;
import java.time.Duration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;


public class Core {
	private CqlSession session;
	public Core(String socketAddress, String dataCenter) {
		try {
			DriverConfigLoader loader =
				    DriverConfigLoader.programmaticBuilder()
				    	.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
//				        .startProfile("binhnguyen")
//				        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
//				        .endProfile()
				        .build();
			
			session = CqlSession.builder().
					addContactPoint(new InetSocketAddress(socketAddress, 9042)).
					withLocalDatacenter(dataCenter).
					withKeyspace("binhnguyen").
					withConfigLoader(loader).
					build();
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public ResultSet getData(int warrantyYear) {
		ResultSet resultSet = session.execute(String.format("SELECT * FROM devices WHERE warrantyyear = %d;", warrantyYear));
		return resultSet;
	}
	
	public void insertDevice(String[] dataArray, int warranyYear, int length) {
		
		StringBuilder query = new StringBuilder("BEGIN BATCH \n");
		for (int i=0; i < length; i++) {
			query.append(String.format("INSERT INTO devices (data, warrantyyear) VALUES "
					+ "(\'%s\' , %d) ; \n",  dataArray[i], warranyYear));
		}
		query.append("APPLY BATCH;");
		
		session.execute(query.toString());
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

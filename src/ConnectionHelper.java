import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionHelper 
{

	public ConnectionHelper() 
	{
		// TODO Auto-generated constructor stub
	}
	

	public static Connection createConnection(String connectionUrl,
			Properties connectionProps)  throws Exception 
	{
		return DriverManager.getConnection(connectionUrl, connectionProps);
	}
	

}

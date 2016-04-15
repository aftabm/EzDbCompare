


public class Main 
{
	public Main() 
	{
	}

	public static void main(String[] args) 
	{
		if (args.length < 5)
		{
			System.out.println("usage : configfile=rules.xml  db1u=?  db1p=?   db2u=?  db2p=?");
			System.exit(-1);
		}
			
		Database db1=null;
		Database db2=null;
		String configFile=null;
		String db1u=null;
		String db1p=null;
		String db2u=null;
		String db2p=null;

		System.out.println("-----------------------------------------------------------------------------");
		System.out.println("EZ DB COMPARE (V 3.0");
		System.out.println("Feedback/bugs email to: aftab.mahmood@citrix.com");
		System.out.println("-----------------------------------------------------------------------------");
		System.out.println("");
		System.out.println("------------------- START COMPARING DATABSES --------------------------------");
	
		
		for(String arg: args)
		{
			String[] argEntry = arg.split("=");
			
			switch(argEntry[0])
			{
				case  "configfile":
					configFile=argEntry[1];
					System.out.println("Config File : "+configFile);
					break;
				case  "db1u":
					db1u = argEntry[1];
					System.out.println("db1 username : "+db1u);
					break;
				case  "db1p":
					db1p=argEntry[1];
					System.out.println("db1 password : ******");
					break;
				case  "db2u":
					db2u=argEntry[1];
					System.out.println("db2 username : "+db2u);
					break;
				case  "db2p":
					db2p=argEntry[1];
					System.out.println("db2 password : ******");
					break;
				default: 
					System.out.println("invalid argument : "+arg);
			}
		}
		
		try 
		{
			
			ConfigReader configReader = new ConfigReader(configFile);
			
			db1 = new Database("1",configReader);
			
			db1.setUsername(db1u);
			db1.setPassword(db1p);
			//db1.setPassword("1.citrix");
			
			db2 = new Database("2",configReader);
			db2.setUsername(db2u);
			db2.setPassword(db2p);
			//db2.setPassword("1.citrix");
			
			System.out.println(db1.getLabel() + " with " +db2.getLabel());
			
			DataComparer.compareTables(db1, db2);
			DataComparer.compareColumns(db1, db2);
			DataComparer.compareColumnDefinition(db1, db2);
			DataComparer.compareTableRowCount(db1, db2);
			DataComparer.compareTableRowData(db1, db2);
			DataComparer.compareCustomSqls(db1, db2);
			
			
			System.out.println("\n");
			System.out.println("------------------- END COMPARING DATABSES --------------------------------");			
			System.out.println("For feedback/bugs email to: aftab.mahmood@citrix.com");
			System.out.println("---------------------------------------------------------------------------");			
			
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
		}
	}
}
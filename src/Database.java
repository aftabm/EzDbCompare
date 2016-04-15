import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Database 
{

	private static final String UNIQUE = "UNIQUE";
	private static final String YES = "YES";
	private static final String ORDER_BY = "ORDER_BY";
	Properties connectionProps = new Properties();
	private String schemaSql;
	private String name;
	private String connectionUrl;
	private Integer ordinal;
	private Connection connection;
	private SortedMap<String, SortedMap<String, SortedMap<String, String>>> metadata = new TreeMap<String, SortedMap<String,SortedMap<String, String>>>();
	private SortedMap<String, String> compareAllSql = new TreeMap<String, String>();
	SortedMap<String, String> compareSqls = new TreeMap<String, String>();
	private TreeSet<String> ignoredTables = new TreeSet<String>();
	private TreeSet<String> ignoredColumnNames = new TreeSet<String>();
	private TreeSet<String> ignoredColumnTypes = new TreeSet<String>();
	private String setIndexSql;
	private ConfigReader configReader;
	private boolean hasInitialized=false;

	public static String CUSTOM_ATTRIBUTE_PREFIX="#";
	public static String ATTRIBUTES = CUSTOM_ATTRIBUTE_PREFIX+"ATTRIBUTES";
	public static String IGNORE = "IGNORE";
	public static String IGNORE_REASON = "IGNORE_REASON";
	public static String REGEX_PREFIX = "REGEX=";
	
	
	public Database() 
	{
		
	    
	    
	}

	public Database(String ordinal, ConfigReader configReader) 
	{
		this.ordinal=Integer.parseInt(ordinal);
		this.configReader = configReader;
		this.configReader.readConfig(this);
	}

	public void setSchemaSql(String sql) {
		this.schemaSql= sql;
		
	}

	public void setName(String name) {
		this.name=name;
		connectionProps.put("databaseName", name);
		
	}

	public void setConnectionUrl(String url) {
		this.connectionUrl = url;
		
	}
	
	public void setUsername(String username)
	{
		connectionProps.put("user", username);
	}
	
	public void setPassword(String password)
	{
		connectionProps.put("password", password);
	}


	public String getConnectionUrl() {
		return this.connectionUrl;
		
	}

	public Properties getConnectionProperties() {
		return this.connectionProps;
	}

	public void setConnection(Connection connection) 
	{
		this.connection=connection;
	}

	public String getSchemaSql() 
	{
		return this.schemaSql;
	}

	public SortedMap<String, SortedMap<String, SortedMap<String, String>>> retrieveSchema() 
	{
		if (hasInitialized)
			return metadata;
			
		try
		{
			if (this.schemaSql==null || this.schemaSql.isEmpty())
			{
				throw new java.lang.IllegalStateException("schemaSql is null");
			}
			
			this.openConnection();

			ResultSet dbSchema = connection.prepareStatement(this.schemaSql).executeQuery();
			System.out.println(getLabel()+": Retrieving schema ................................... ");
			
			while(dbSchema.next()) 
			{
				String tableName = dbSchema.getString("TABLE_NAME").toUpperCase();
				String columnName =dbSchema.getString("COLUMN_NAME").toUpperCase();
				
				SortedMap<String, SortedMap<String, String>> table = metadata.get(tableName);				
				SortedMap<String, String> tableAttributes;
				
				if (table==null)
				{
					table = new TreeMap<String, SortedMap<String, String>>();
					tableAttributes = new TreeMap<String, String>();					
					table.put(ATTRIBUTES, tableAttributes);
					metadata.put(tableName, table);
				}
				else
				{
					tableAttributes = table.get(ATTRIBUTES);
				}
				
				if (this.ignoredTables.contains(tableName))
					tableAttributes.put(IGNORE, YES);				
				
				SortedMap<String, String> columnAttributes = new TreeMap<String, String>();
				
				//column definition
				for (int i=1; i<= dbSchema.getMetaData().getColumnCount();i++)
				{
					if(!columnAttributes.containsKey(dbSchema.getMetaData().getColumnName(i)))
						columnAttributes.put(dbSchema.getMetaData().getColumnName(i), dbSchema.getString(i));
				}
				
				
				table.put(columnName, columnAttributes);

				if(this.shouldIgnoreColumnType(columnAttributes.get("DATA_TYPE")))
					columnAttributes.put(IGNORE, YES);
				
				else if(shouldIgnoreColumnName(tableName, columnName))
					columnAttributes.put(IGNORE, YES);
				
				
				System.out.print(".");
			}
			
			retrieveIndexes();
			
			System.out.println();
			System.out.println("Following tables will be ignored : "+this.ignoredTables.toString());
			System.out.println("Following columns will be ignored : "+this.ignoredColumnNames.toString());
			System.out.println("Following datatypes will be ignored : "+this.ignoredColumnTypes.toString());
			
			System.out.print("\n\r");
			
			hasInitialized =true;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			this.closeConnection();
		}
		
		
		return metadata;
		
	}
	
	
	
	private void retrieveIndexes() 
	{
		
		try
		{
			this.openConnection();

			ResultSet dbSchema = connection.prepareStatement(this.setIndexSql).executeQuery();
			System.out.println();
			System.out.println(getLabel()+": Retrieving Indexes ................................... ");
			System.out.println("Unique columns : ");
			
			while(dbSchema.next()) 
			{
				String tableName = dbSchema.getString("TABLE_NAME").toUpperCase();
				String columnName =dbSchema.getString("COLUMN_NAME").toUpperCase();
				String unique = dbSchema.getString("UNIQUE");
				
				if ("1".equals(unique))
				{
					unique="YES";
				}
				else
				{
					unique="NO";
				}
				
				if (metadata.containsKey(tableName))
				{
					SortedMap<String, SortedMap<String, String>> table = metadata.get(tableName);				
					
					if (table.containsKey(columnName))
					{
						table.get(columnName).put(UNIQUE, unique);
						
						System.out.println("table : "+tableName+" column : "+columnName);
					}
				}
				
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			this.closeConnection();
		}
		
	}	
	
	
	
	

	public boolean shouldIgnoreColumnName(String tableName, String columnName) 
	{
		boolean ignore = this.ignoredColumnNames.contains(columnName);
		
		if(ignore==false)
		{
			ignore =YES.equalsIgnoreCase(metadata.get(tableName).get(columnName).get(IGNORE));
		}
		
		if(ignore==false)
		{
			for (String ignoreColumn : this.ignoredColumnNames)
			{
				if (ignoreColumn.startsWith(REGEX_PREFIX))
				{
					Pattern pattern = Pattern.compile(ignoreColumn.replace(REGEX_PREFIX, ""));
					Matcher matcher = pattern.matcher(columnName);	
					ignore = ignore || matcher.find();
				}
			}
		}
		
		return ignore;
	}
	
	
	
	public boolean shouldIgnoreColumnType(String type) 
	{
		return this.ignoredColumnTypes.contains(type.toUpperCase()); 
	}	

	public Connection openConnection() throws Exception 
	{
		if(connection==null || connection.isClosed())
		{
			connection = ConnectionHelper.createConnection(this.connectionUrl, this.connectionProps);
		}
		
		return this.connection;
		
	}

	public void closeConnection() 
	{
		try 
		{
			if (connection!=null)
				connection.close();
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
	}

	public String getLabel() 
	{
		return this.name+"-"+this.ordinal;
	}

	public void addCompareAllSql(String name, String sql) 
	{
		this.compareAllSql.put(name, sql);
	}

	public String getCompareAllSql(String name) 
	{
		return this.compareAllSql.get(name);
	}


	public void addCompareSql(String label, String sql ) 
	{
		this.compareSqls.put(label, sql);
	}
	
	public String getCompareSql(String name ) 
	{
		return this.compareSqls.get(name);
	}	
	
	public SortedMap<String, String> getAllCompareSql( ) 
	{
		return this.compareSqls;
	}

	public int getOrdinal() 
	{
		return this.ordinal.intValue();
	}

	public void addIgnoreTable(String tableName, String reason) 
	{
		if(metadata.containsKey(tableName))
		{
			metadata.get(tableName).get(ATTRIBUTES).put(IGNORE, YES);
		}
		
		this.ignoredTables.add(tableName);		
	}

	public void addIgnoreColumnName(String columnName) 
	{		
		this.ignoredColumnNames.add(columnName);
	}
	
	public void addIgnoreColumn(String tablename, String columnName, String reason) 
	{
		if(metadata.containsKey(tablename))
			if (metadata.get(tablename).containsKey(columnName))
			{
				metadata.get(tablename).get(columnName).put(IGNORE, YES);
				metadata.get(tablename).get(columnName).put(IGNORE_REASON, reason);
			}
	}	

	public SortedMap<String, SortedMap<String, String>> getTable(String tableName) 
	{
		return this.metadata.get(tableName);
		
	}

	public Set<String> getColumnNames(String tableName) 
	{
		SortedMap<String, SortedMap<String, String>> table = this.metadata.get(tableName);		
		return table.keySet();
	}

	public String generateSql(String tableName, String inSql) 
	{
		StringBuilder columnNames = new StringBuilder();
		
		boolean isFirst = true;
		String orderByColumnName ="";//this.metadata.get(tableName).get("--FIRST_NOTNULL_COLUMN_NAME--");
		String firstColumn="";
		
/*		if(orderByColumnName!=null && !orderByColumnName.isEmpty())
			orderByColumnName = "\""+orderByColumnName+"\"";*/
		
		SortedMap<String, SortedMap<String, String>> table = this.metadata.get(tableName);
		
		
		System.out.print("Generating SQL : Column ignored : ");
				
		for (Entry<String, SortedMap<String, String>> column : table.entrySet())
		{			
			String columnName = column.getKey();
			
			if(columnName.startsWith(CUSTOM_ATTRIBUTE_PREFIX))
				continue;
			
			if(shouldIgnoreColumnName(tableName,columnName))
			{
				System.out.print(columnName+", ");
				continue;
			}
			
			SortedMap<String, String> columnDef = column.getValue();
			
			String orderBy = columnDef.get(ORDER_BY);
				
			if (YES.equalsIgnoreCase(orderBy))
				orderByColumnName = "\""+columnName+"\"";				
						
			if (orderByColumnName==null || orderByColumnName.isEmpty())
			{
				String unique = columnDef.get(UNIQUE);
				
				if (YES.equalsIgnoreCase(unique))
					orderByColumnName = "\""+columnName+"\"";
			}
			
			if (!isFirst)
				columnNames.append(",");
			else
				firstColumn=columnName;
			
			columnNames.append("\"");
			columnNames.append(columnName);
			columnNames.append("\"");
			
			isFirst=false;
			
		}
		
		System.out.println("");

		if (!columnNames.toString().trim().isEmpty())
		{
			
			String outSql = inSql.replace("TABLE_NAME", "\""+tableName+"\"");
			outSql = outSql.replace("COLUMN_NAME", columnNames.toString().trim());
			
			if (orderByColumnName==null || orderByColumnName.isEmpty())
			{
				orderByColumnName = "\""+firstColumn+"\"";
				System.out.println("!!WARNING!!! Comparesion may be incorrect. Please check ORDERY_BY column.");
			}
			
			outSql = outSql.replace("ORDER_BY_COLUMN", orderByColumnName);
			
			return outSql;
		}
		
		return null;
		
	}

	public void addIgnoreColumnType(String type) 
	{
		this.ignoredColumnTypes.add(type);
	}

	public boolean shouldIgnoreTable(String tableName) 
	{
		return this.ignoredTables.contains(tableName);
	}

	public void setIndexSql(String sql) 
	{
		this.setIndexSql= sql;
		
	}

	public void addOrderByColumn(String tableName, String columnName) 
	{
		if(metadata.containsKey(tableName))
		{
			if (metadata.get(tableName).containsKey(columnName))
			{
				metadata.get(tableName).get(columnName).put(ORDER_BY, YES);
			}
		}
	}

}

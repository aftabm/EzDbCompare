import java.sql.ResultSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;


public class DataComparer 
{

	private DataComparer() 
	{

	
	}

	public static void compareTables(Database db1, Database db2) 
	{
		try
		{
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db1Schema = db1.retrieveSchema();
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db2Schema = db2.retrieveSchema();
			
			if(db1Schema==null || db1Schema.size()==0)
				return;
			
			if(db2Schema==null || db2Schema.size()==0)
				return;
			
			
			Set<String> db1TableNames = db1Schema.keySet();
			Set<String> db2TableNames = db2Schema.keySet();

			//compare tables
			System.out.println("");
			System.out.println("------------------- COMPARING TABLE --------------------------------");
			
			for (String tableName : db1TableNames)
			{
				if (!db2TableNames.contains(tableName))
				{
					db1.addIgnoreTable(tableName, "not found in other DB");
					db2.addIgnoreTable(tableName, "not found in other DB");
					System.out.println(db2.getLabel() + " : "+"table removed : "+tableName);
				}
			}
			
			for (String tableName : db2TableNames)
			{
				if (!db1TableNames.contains(tableName))
				{
					db1.addIgnoreTable(tableName, "not found in other DB");
					db2.addIgnoreTable(tableName, "not found in other DB");
					System.out.println(db2.getLabel() + " : "+"table added : "+tableName);
				}
			}//for
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void compareColumns(Database db1, Database db2) 
	{
		
		try
		{
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db1Schema = db1.retrieveSchema();
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db2Schema = db2.retrieveSchema();
			
			
			if(db1Schema==null || db1Schema.size()==0)
				return;
			
			if(db2Schema==null || db2Schema.size()==0)
				return;
			
			
			Set<String> db1TableNames = db1Schema.keySet();
			Set<String> db2TableNames = db2Schema.keySet();
			
			
			System.out.println("");
			System.out.println("------------------- COMPARING COLUMNS --------------------------------");
			
			
			for (String tableName : db1TableNames)
			{
				if (db2TableNames.contains(tableName))
				{
					
					Map<String, SortedMap<String, String>> db1Table = db1Schema.get(tableName);
					Map<String, SortedMap<String, String>> db2Table = db2Schema.get(tableName);
					
					Set<String> db1Columns = db1Table.keySet();
					Set<String> db2Columns = db2Table.keySet();
					
					for(String columnName : db1Columns)
					{
						if(columnName.startsWith(db1.CUSTOM_ATTRIBUTE_PREFIX))
							continue;
						
						if (!db2Columns.contains(columnName))
						{
							System.out.println(db2.getLabel() + " : "+tableName+" : column removed " + " : "+columnName);
							
							db1.addIgnoreColumn(tableName, columnName, "not found in other DB");
							db2.addIgnoreColumn(tableName, columnName, "not found in other DB");
						}						
					}//for
					
					
					for(String columnName : db2Columns)
					{
						if(columnName.startsWith(db2.CUSTOM_ATTRIBUTE_PREFIX))
							continue;

						if (!db1Columns.contains(columnName))
						{
							db1.addIgnoreColumn(tableName, columnName, "not found in other DB");
							db2.addIgnoreColumn(tableName, columnName, "not found in other DB");
							System.out.println(db2.getLabel() + " : "+tableName+" : column added " + " : "+columnName);
						}						
					}//for
				}//if
			}//for
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void compareColumnDefinition(Database db1, Database db2) 
	{
		try
		{
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db1Schema = db1.retrieveSchema();
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db2Schema = db2.retrieveSchema();
			
			if(db1Schema==null || db1Schema.size()==0)
				return;
			
			if(db2Schema==null || db2Schema.size()==0)
				return;
			
			Set<String> db1TableNames = db1Schema.keySet();
			Set<String> db2TableNames = db2Schema.keySet();
			
			System.out.println("");
			System.out.println("------------------- COMPARING COLUMNS DEFINITIONS--------------------------------");
			
			for (String tableName : db1TableNames)
			{
				if (db2TableNames.contains(tableName))
				{
					Map<String, SortedMap<String, String>> db1Table = db1Schema.get(tableName);
					Map<String, SortedMap<String, String>> db2Table = db2Schema.get(tableName);
					
					Set<String> db1Columns = db1Table.keySet();
					Set<String> db2Columns = db2Table.keySet();
					
					for(String columnName : db1Columns)
					{
						if(columnName.startsWith(db1.CUSTOM_ATTRIBUTE_PREFIX))
							continue;
							
						if (db2Columns.contains(columnName))
						{
							if(columnName.startsWith(db1.CUSTOM_ATTRIBUTE_PREFIX))
								continue;
							
							SortedMap<String, String> db1ColDef = db1Table.get(columnName);
							SortedMap<String, String> db2ColDef = db2Table.get(columnName);
							
							if (!db1ColDef.equals(db2ColDef))
							{								
								System.err.println("column definition changed : tableName: "+tableName+" columnName : "+columnName);								
								System.out.println(db1.getLabel() + " :\t "+ db1ColDef);
								System.out.println(db2.getLabel() + " :\t "+ db2ColDef);
								System.out.println("");
							}							
						}
					}
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}		
	}

	public static void compareTableRowCount(Database db1, Database db2) 
	{
		try
		{	
			String compareAllSql = db1.getCompareAllSql("row_count");
			
			if (compareAllSql==null || compareAllSql.isEmpty())
			{
				return;
			}
			
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db1Schema = db1.retrieveSchema();
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db2Schema = db2.retrieveSchema();
			
			
			if(db1Schema==null || db1Schema.size()==0)
				return;
			
			if(db2Schema==null || db2Schema.size()==0)
				return;
			
			
			Set<String> db1TableNames = db1Schema.keySet();
			Set<String> db2TableNames = db2Schema.keySet();
			
			
			System.out.println("");
			System.out.println("------------------- COMPARING ROW COUNTS --------------------------------");
			System.out.println(compareAllSql);
			
			for (String tableName : db1TableNames)
			{
				if (db2TableNames.contains(tableName))
				{
					
					ResultSet db1Result = db1.openConnection().prepareStatement(compareAllSql.replace("TABLE_NAME", tableName)).executeQuery();
					int db1RowCount=0;
					
					if (db1Result.next())					
						db1RowCount = db1Result.getInt(1);
					
					ResultSet db2Result = db2.openConnection().prepareStatement(compareAllSql.replace("TABLE_NAME", tableName)).executeQuery();
					int db2RowCount=0;
					
					if (db2Result.next())					
						db2RowCount = db2Result.getInt(1);
					
					if (db1RowCount!=db2RowCount)
					{
						System.out.println(tableName);
						System.out.println(db1.getLabel() + " :\t "+ db1RowCount);
						System.out.println(db2.getLabel() + " :\t "+ db2RowCount);
						System.out.println("");
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
			db1.closeConnection();
			db2.closeConnection();
		}
	}

	public static void compareCustomSqls(Database db1, Database db2) 
	{
		try
		{	
			System.out.println("");
			System.out.println("------------------- COMPARING USER SQLS --------------------------------");
			
			for (String sqlName : db1.getAllCompareSql().keySet())
			{
				String db1Sql = db1.getCompareSql(sqlName);
				String db2Sql = db2.getCompareSql(sqlName);
				
				if (db1Sql==null || db1Sql.isEmpty())
					return;
				
				if (db2Sql==null || db2Sql.isEmpty())
					return;				
				
				System.out.println("db1Sql " + db1Sql);
				System.out.println("db2Sql " + db2Sql);
				
				ResultSet db1Result = db1.openConnection().prepareStatement(db1Sql).executeQuery();
				ResultSet db2Result = db2.openConnection().prepareStatement(db2Sql).executeQuery();
				
				boolean columnNameExtracted = false;
				boolean db1HasData=true;
				boolean db2HasData=true;
				
				while(db1HasData||db2HasData) 
				{
					StringBuilder db1Row = new StringBuilder();
					StringBuilder db2Row = new StringBuilder();
					StringBuilder dbColumnNames = new StringBuilder();
					
					if(db1Result.next())
					{
						for (int i=1; i<= db1Result.getMetaData().getColumnCount();i++)
						{
							db1Row.append(db1Result.getString(i));
							db1Row.append("\t");
							
							if(!columnNameExtracted)
							{
								dbColumnNames.append(db1Result.getMetaData().getColumnName(i));
								dbColumnNames.append("\t");
							}
						}
						columnNameExtracted=true;
					}
					else
					{
						db1HasData=false;
					}
							
					if(db2Result.next())
					{
						for (int i=1; i<= db2Result.getMetaData().getColumnCount();i++)
						{
							db2Row.append(db2Result.getString(i));
							db2Row.append("\t");
							
/*							if(!columnNameExtracted)
							{
								dbColumnNames.append(db1Result.getMetaData().getColumnName(i+1));
								dbColumnNames.append("\t");
							}
*/						}
					}
					else
					{
						db2HasData=false;						
					}
					
					
					if(db1Row.toString().compareTo(db2Row.toString())!=0)
					{
						System.err.println("Row did not match");
						System.out.println(dbColumnNames);
						System.out.println(db1.getLabel()+" : "+db1Row);
						System.out.println(db2.getLabel()+" : "+db2Row);
						
						break;
					}
					else
					{
						System.out.print(".");
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
			db1.closeConnection();
			db2.closeConnection();
		}		
	}

	public static void compareTableRowData(Database db1, Database db2) 
	{
		try
		{	
			String compareAllSql = db1.getCompareAllSql("row_data");
			
			if (compareAllSql==null || compareAllSql.isEmpty())
			{
				return;
			}
			
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db1Schema = db1.retrieveSchema();
			SortedMap<String, SortedMap<String, SortedMap<String, String>>> db2Schema = db2.retrieveSchema();
			
			
			if(db1Schema==null || db1Schema.size()==0)
				return;
			
			if(db2Schema==null || db2Schema.size()==0)
				return;
			
			
			Set<String> db1TableNames = db1Schema.keySet();
			Set<String> db2TableNames = db2Schema.keySet();
			
			
			System.out.println("");
			System.out.println("------------------- COMPARING TABLE ROWS --------------------------------");
			System.out.println(compareAllSql);
			
			for (String tableName : db1TableNames)
			{
				if (db2TableNames.contains(tableName))
				{
					System.out.println("");
					
					System.out.println("------------------------------------------------------------------------");
					System.out.println("Comparing row data for table : "+tableName);
					System.out.println("------------------------------------------------------------------------");
					
					if(db1.shouldIgnoreTable(tableName) ||db2.shouldIgnoreTable(tableName))
					{
						System.out.println(tableName+" found in ignore table list. Skipping");
						continue;
					}
					
					String db1Sql = db1.generateSql(tableName, compareAllSql);
					String db2Sql = db2.generateSql(tableName, compareAllSql);
					
					System.out.println("");
					System.out.println("db1 SQL : "+db1Sql);
					System.out.println("db2 SQL : "+db2Sql);
					
					if (db1Sql==null || db1Sql.isEmpty())
						continue;
					
					if (db2Sql==null || db2Sql.isEmpty())
						continue;					
					
					ResultSet db1Result = db1.openConnection().prepareStatement(db1Sql).executeQuery();
					ResultSet db2Result = db2.openConnection().prepareStatement(db2Sql).executeQuery();
					
					boolean columnNameExtracted = false;
					boolean db1HasData=true;
					boolean db2HasData=true;
					
					while(db1HasData||db2HasData) 
					{
						StringBuilder db1Row = new StringBuilder();
						StringBuilder db2Row = new StringBuilder();
						StringBuilder dbColumnNames = new StringBuilder();
						
						if(db1Result.next())
						{
							for (int i=1; i<= db1Result.getMetaData().getColumnCount();i++)
							{
								
								String dbColumnName = db1Result.getMetaData().getColumnName(i);
								String result = db1Result.getString(i);
								
								if (result!=null && !result.isEmpty())
								{
									db1Row.append(result.trim());
									db1Row.append("\t");
								}
								
								if(!columnNameExtracted)
								{
									
									if(dbColumnName!=null && !dbColumnName.isEmpty())
									{
										dbColumnNames.append(dbColumnName.trim());
										dbColumnNames.append("\t");
									}
								}
							}
							columnNameExtracted=true;
						}
						else
						{
							db1HasData=false;
						}
								
						if(db2Result.next())
						{
							for (int i=1; i<= db2Result.getMetaData().getColumnCount();i++)
							{
								
								String result = db2Result.getString(i);
								
								if (result!=null && !result.isEmpty())
								{
									db2Row.append(result.trim());
									db2Row.append("\t");
								}
								
								if(!columnNameExtracted)
								{
									dbColumnNames.append(db2Result.getMetaData().getColumnName(i).trim());
									dbColumnNames.append("\t");									
								}
							}
						}
						else
						{
							db2HasData=false;						
						}
						
						db1Row.trimToSize();
						db2Row.trimToSize();
						if(db1Row.toString().compareTo(db2Row.toString())!=0)
						{
							System.out.println("");
							System.err.println("Row did not match for table : "+ tableName);
							
							if (dbColumnNames!=null && dbColumnNames.length()>0)
								System.out.println(dbColumnNames);
							System.out.println(db1.getLabel()+" : "+db1Row);
							System.out.println(db2.getLabel()+" : "+db2Row);
							
							break;
						}
						else
							System.out.print(".");
						
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
			db1.closeConnection();
			db2.closeConnection();
		}		
		
	}
}

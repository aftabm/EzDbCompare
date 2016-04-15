import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class ConfigReader 
{
	protected final String filename;
	protected Document doc;
	
	
	public ConfigReader(String filename) throws Exception 
	{
		this.filename = filename;
		
		InputStream inputStream = ClassLoader.getSystemResourceAsStream(filename);
		
		DocumentBuilderFactory docfactory = DocumentBuilderFactory.newInstance();
		docfactory.setIgnoringElementContentWhitespace(true);
		docfactory.setIgnoringComments(true);
					
		DocumentBuilder dBuilder = docfactory.newDocumentBuilder();
		
		doc = dBuilder.parse(inputStream);		
		doc.normalize();
		
	}
	
	
	public Database readConfig(Database database)
	{
		
		try
		{
			XPathFactory xPathfactory = XPathFactory.newInstance();			
			XPath xpath = xPathfactory.newXPath();
			
			XPathExpression expr;
			
			expr = xpath.compile("config/db"+database.getOrdinal()+"/@name");
			String result = expr.evaluate(doc).trim();
			System.out.println("config/db"+database.getOrdinal()+"/name: "+result);
			database.setName(result);
			
			expr = xpath.compile("config/db"+database.getOrdinal()+"/connectionUrl");
			result = expr.evaluate(doc).trim();
			System.out.println("config/db"+database.getOrdinal()+"/connectionUrl: "+result);
			database.setConnectionUrl(result);
			
			
			readSchemaRetrivalSql(database);
			readIndexRetrivalSql(database);
			readIgnoreTablesList(database);
			readRowCountSqls(database);
			readRowCompareSql(database);
			readCustomSqls(database);
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
			database = null;
		}
		finally
		{
		}
		
		return database;
	}
	
	
	public void readSchemaRetrivalSql(Database database) throws Exception
	{
		try
		{
			XPathFactory xPathfactory = XPathFactory.newInstance();			
			XPath xpath = xPathfactory.newXPath();
			
			XPathExpression expr = xpath.compile("config/schemaRetrivalSql");		
			String result = expr.evaluate(doc).replace("\n", " ").replace("\t", "").replace("\r", " ");
			System.out.println("config/schemaRetrivalSql: "+result);
			database.setSchemaSql(result);
			
		}
		finally
		{
		}
		
	}
	
	
	public void readIndexRetrivalSql(Database database) throws Exception
	{
		try
		{
			XPathFactory xPathfactory = XPathFactory.newInstance();			
			XPath xpath = xPathfactory.newXPath();
			
			XPathExpression expr = xpath.compile("config/indexRetrivalSql");		
			String result = expr.evaluate(doc).replace("\n", " ").replace("\t", "").replace("\r", " ");
			System.out.println("config/schemaRetrivalSql: "+result);
			database.setIndexSql(result);
			
		}
		finally
		{
		}
		
	}	
	
	
	
	public static void close()
	{
		
	}
	

	
	public void readIgnoreTablesList(Database database) throws Exception
	{
		try
		{
			XPathFactory xPathfactory = XPathFactory.newInstance();			
			XPath xpath = xPathfactory.newXPath();
			
			XPathExpression expr = null;
			
			expr = xpath.compile("config/db"+database.getOrdinal()+"/ignoreTable");
			NodeList ignoreTables = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
			
			for (int i = 0; i < ignoreTables.getLength(); i++) 
			{
				String ignoreTablename = ignoreTables.item(i).getTextContent().replaceAll("\"", "").trim();
				
				if (ignoreTablename!=null && !ignoreTablename.isEmpty())
					database.addIgnoreTable(ignoreTablename, "uesr ignored");
			}
			
		}
		finally
		{
		}
	}	


	
	public void readRowCountSqls(Database database) throws Exception
	{
		
		try
		{
			XPathFactory xPathfactory = XPathFactory.newInstance();			
			XPath xpath = xPathfactory.newXPath();
			
			XPathExpression expr = null;
			

			expr = xpath.compile("config/evaluate/compareAll[contains(@name,'row_count')]/sql");
			String result = expr.evaluate(doc).trim();
			System.out.println("config/db"+database.getOrdinal()+"/name: "+result);
			database.addCompareAllSql("row_count", result);
		}
		finally
		{
		}
	}
	
	
	
	public void readRowCompareSql(Database database) throws Exception
	{
		
		try
		{
			XPathFactory xPathfactory = XPathFactory.newInstance();			
			XPath xpath = xPathfactory.newXPath();
			
			XPathExpression expr = null;
			
			expr = xpath.compile("config/evaluate/compareAll[contains(@name,'row_data')]/sql");
			String result = expr.evaluate(doc).trim();
			System.out.println("config/db"+database.getOrdinal()+"/name: "+result);
			database.addCompareAllSql("row_data", result);
			
			expr = xpath.compile("config/evaluate/compareAll[contains(@name,'row_data')]/ignoreColumnName");
			NodeList ignoreColumnNames = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
			
			for (int i = 0; i < ignoreColumnNames.getLength(); i++) 
			{
				String ignoreColumn = ignoreColumnNames.item(i).getTextContent().replaceAll("\"", "").trim();
				
				if (ignoreColumn!=null && !ignoreColumn.isEmpty())
					database.addIgnoreColumnName(ignoreColumn.toUpperCase());//"user ignored"
			}
			
			expr = xpath.compile("config/evaluate/compareAll[contains(@name,'row_data')]/ignoreColumnType");
			NodeList ignoreColumnTypes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
			
			for (int i = 0; i < ignoreColumnTypes.getLength(); i++) 
			{
				String ignoreColumnType = ignoreColumnTypes.item(i).getTextContent().replaceAll("\"", "").trim();
				
				if (ignoreColumnType!=null && !ignoreColumnType.isEmpty())
					database.addIgnoreColumnType(ignoreColumnType.toUpperCase());
			}	
			
			
			
			expr = xpath.compile("config/evaluate/compareAll[contains(@name,'row_data')]/order_by");
			NodeList orderByColumns = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
			
			for (int i = 0; i < orderByColumns.getLength(); i++) 
			{
				
				expr = xpath.compile("table");
				String tableName  = expr.evaluate(orderByColumns.item(i)).trim();
				
				expr = xpath.compile("column");
				String columnName  = expr.evaluate(orderByColumns.item(i)).trim();
				
				database.addOrderByColumn(tableName, columnName);
				
			}	
			
			
			
			
		}
		finally
		{
		}
	}

	
	
	
	public void readCustomSqls(Database database) throws Exception
	{
		try
		{								
			XPathFactory xPathfactory = XPathFactory.newInstance();			
			XPath xpath = xPathfactory.newXPath();
			
			XPathExpression expr = null;
			
			expr = xpath.compile("config/evaluate/compare");
			NodeList compareSqls = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
			
			for (int i = 0; i < compareSqls.getLength(); i++) 
			{
				expr = xpath.compile("@name");
				String compareLabel  = expr.evaluate(compareSqls.item(i)).trim();
				
				String sql=null;
				
				if (database.getOrdinal()==1)
				{
					expr = xpath.compile("db1Sql");
					sql  = expr.evaluate(compareSqls.item(i)).trim();
				}
				else if (database.getOrdinal()==2)
				{
					expr = xpath.compile("db2Sql");
					sql  = expr.evaluate(compareSqls.item(i)).trim();
				}
			     
				if(sql!=null)
					database.addCompareSql(compareLabel, sql);
			}
			
		}
		finally
		{
		}
	}
	

}

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
public class DBApp implements DBAppInterface{

	private Vector<Table> tables = new Vector<Table>();
	
	@Override
	public void init() {

	}
//	public void ser(Table e) throws IOException {
//		try{		FileOutputStream fOut = new FileOutputStream("/tmp/table.ser");
//		ObjectOutputStream out = new ObjectOutputStream(fOut);
//		out.writeObject(e);
//		out.close();
//		fOut.close();}catch(IOException i) {
//		i.printStackTrace();	
//		}
//	}

	@Override
	public void createTable(String strTableName, 
			 String strClusteringKeyColumn, 
			Hashtable<String,String> htblColNameType, 
			Hashtable<String,String> htblColNameMin, 
			Hashtable<String,String> htblColNameMax ) 
			 throws DBAppException {
		tables.forEach((c)->{
			if(c.getName().equals(strTableName)) {			
					return;				
				}
			
		});
	Table createdTable = new Table(strTableName,strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
		this.tables.add(createdTable);
	}

	@Override
	public void createIndex(String strTableName, String[] strarrColumnNames) throws DBAppException {
		
		
	}

	@Override
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables.forEach((c)->{
			if(c.getName().equals(strTableName)) {		
				String key = c.getTableKey();
					try {
						
						c.insertSortedTuple(htblColNameValue,strTableName,key);
					} catch (DBAppException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					
					}				
				}
			
		});
		
//		throw new DBAppException("");
		
	}

	@Override
	public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColumnNameValue)
			throws DBAppException {
		tables.forEach((c) -> {
			String key = c.getTableKey();
			if (c.getName().equals(strTableName)) {
				
				try {
				c.updateTuple(strClusteringKeyValue,htblColumnNameValue,strTableName,key);
				return;
				}catch(DBAppException e){
				
				}

			} 

		});
		
		
	}

	@Override
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables.forEach((c) -> {
			String key = c.getTableKey();
			if (c.getName().equals(strTableName)) {
				
				c.deleteTuple(htblColNameValue,strTableName,key);
			}

		});
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		// TODO Auto-generated method stub
		return null;
	}
	@SuppressWarnings("deprecation")
	public static void main(String[]args) throws DBAppException {
		String strTableName = "Student"; 
		DBApp dbApp = new DBApp( ); 
		Hashtable htblColNameType = new Hashtable( ); 
		htblColNameType.put("id", "java.lang.Integer"); htblColNameType.put("name", "java.lang.String"); 
		htblColNameType.put("gpa", "java.lang.Double"); 
		dbApp.createTable( strTableName, "id", htblColNameType,htblColNameType,htblColNameType); 
		dbApp.createIndex( strTableName, new String[] {"gpa"} ); 
		Hashtable<String, Object> htblColNameValue = new Hashtable( ); 
		htblColNameValue.put("id", new Integer( 2343432 )); 
		htblColNameValue.put("name", new String("Ahmed Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 0.95 ) ); 
		dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		htblColNameValue.clear( ); 
		htblColNameValue.put("id", new Integer( 453455 )); 
		htblColNameValue.put("name", new String("Ahmed Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 0.95 ) ); 
		dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		htblColNameValue.clear( ); 
		htblColNameValue.put("id", new Integer( 5674567 )); 
		htblColNameValue.put("name", new String("Dalia Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 1.25 ) ); 
		dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		htblColNameValue.clear( ); 
		htblColNameValue.put("id", new Integer( 23498 )); 
		htblColNameValue.put("name", new String("John Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 1.5 ) ); 
		dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		htblColNameValue.clear( ); 
		htblColNameValue.put("id", new Integer( 78452 )); 
		htblColNameValue.put("name", new String("Zaky Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 0.88 ) ); 
		dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		SQLTerm[] arrSQLTerms; 
		arrSQLTerms = new SQLTerm[2]; 
		arrSQLTerms[0]._strTableName = "Student"; 
		arrSQLTerms[0]._strColumnName= "name"; 
		arrSQLTerms[0]._strOperator = "="; 
		arrSQLTerms[0]._objValue = "John Noor"; 
		arrSQLTerms[1]._strTableName = "Student"; 
		arrSQLTerms[1]._strColumnName= "gpa"; 
		arrSQLTerms[1]._strOperator = "="; 
		arrSQLTerms[1]._objValue = new Double( 1.5 ); 
		String[]strarrOperators = new String[1]; 
		strarrOperators[0] = "OR"; 
		// select * from Student where name = “John Noor” or gpa = 1.5; 
		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators); 
		dbApp.deleteFromTable(strTableName, htblColNameValue);
	}
}

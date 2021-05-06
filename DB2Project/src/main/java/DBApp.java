import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
public class DBApp implements DBAppInterface{

	private ArrayList<Table> tables = new ArrayList<Table>();
	
	@Override
	public void init() {

	}


	@Override
	public void createTable(String strTableName, 
			 String strClusteringKeyColumn, 
			Hashtable<String,String> htblColNameType, 
			Hashtable<String,String> htblColNameMin, 
			Hashtable<String,String> htblColNameMax ) 
			 throws DBAppException {

		System.out.println(tables);
	Table createdTable = new Table(strTableName,strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
		this.tables.add(createdTable);
		writeTables(tables);
		
	}
	public void writeTables(ArrayList<Table> tmpTables) {

		try {
			FileOutputStream fileOut = new FileOutputStream("./src/main/resources/data/" + "tablesArray" + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(tmpTables);
			out.close();
			fileOut.close();
//			System.out.println("Serialized data is saved in " + strTableName + " P" + indicator + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public ArrayList<Table> readTables() {

		try {
			FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + "tablesArray" + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			ArrayList<Table> e = (ArrayList<Table>) in.readObject();
			System.out.println(e);
		

			in.close();
			fileIn.close();
			return e;
		} catch (IOException i) {
			i.printStackTrace();
			return null;

		} catch (ClassNotFoundException c) {
			System.out.println("Page class not found");
			c.printStackTrace();
			return null;
		}
	}
	@Override
	public void createIndex(String strTableName, String[] strarrColumnNames) throws DBAppException {
		
		
	}

	@Override
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables = readTables();
	
		System.out.println(htblColNameValue);
		ArrayList<String> tmpNames = new ArrayList<String>();
		for(Table table:tables) {
		
			tmpNames.add(table.getName());
			if(table.getName().equals(strTableName)) {
				String key = table.getTableKey();
				Set<String>	names= table.getHtblColNameType().keySet();
				Set<String>	insertNames =  htblColNameValue.keySet();
				System.out.println(names);
				System.out.println(insertNames);
				for(String name: insertNames) {
					if(!names.contains(name)) {
						throw new DBAppException();
					}
				}
				for(String name: names) {
					if(!insertNames.contains(name)) {
						return;
					}
					
				}
				table.insertSortedTuple(htblColNameValue, strTableName,key);
				return;
			}
		}
		if(!tmpNames.contains(strTableName)) {
			throw new DBAppException();
		}
		
		
		
//		throw new DBAppException("Table not found");
		
		
		
	}

	@Override
	public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		
		tables = readTables();

		ArrayList<String> tmpNames = new ArrayList<String>();
		for(Table table:tables) {
			
			String key = table.getTableKey();
			if(table.getName().equals(strTableName)){
				Set<String>	names= table.getHtblColNameType().keySet();
				Set<String>	insertNames =  htblColNameValue.keySet();
				System.out.println(names);
				System.out.println(insertNames);
				for(String name: insertNames) {
					if(!names.contains(name)) {
						throw new DBAppException();
					}
				}
				for(String name: names) {
					if(!insertNames.contains(name)) {
						return;
					}
					
				}
				table.updateTuple(strClusteringKeyValue,htblColNameValue,strTableName,key);
				return;
			}
			tmpNames.add(table.getName());
		}

		if(!tmpNames.contains(strTableName)) {
			throw new DBAppException();
		}
		
		
		
	}

	@Override
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables = readTables();
		ArrayList<String> tmpNames = new ArrayList<String>();
		for(Table table:tables) {
			if(table.getName().equals(strTableName)) {
				String key = table.getTableKey();
				if(table.getName().equals(strTableName)) {
					Set<String>	names= table.getHtblColNameType().keySet();
					Set<String>	insertNames =  htblColNameValue.keySet();
					System.out.println(names);
					System.out.println(insertNames);
					for(String name: insertNames) {
						if(!names.contains(name)) {
							throw new DBAppException();
						}
						
					}
					for(String name: names) {
						if(!insertNames.contains(name)) {
							return;
						}
						
					}
					table.deleteTuple(htblColNameValue, strTableName, key);
				}
			}
			tmpNames.add(table.getName());
		}
		if(!tmpNames.contains(strTableName)) {
			throw new DBAppException();
		}
		
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

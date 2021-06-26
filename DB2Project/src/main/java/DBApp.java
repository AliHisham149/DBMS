import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Vector;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.validator.ValidateWith;

import java.io.*;
import java.math.RoundingMode;
import java.nio.BufferUnderflowException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
public class DBApp implements DBAppInterface{

	private Vector<Table> tables = new Vector<Table>();
	private Vector <GridIndex> indices = new Vector <GridIndex>();
	public Vector<GridIndex> getIndices() {
		return indices;
	}


	public void setIndices(Vector<GridIndex> indices) {
		this.indices = indices;
	}





	public static boolean insertedAnIndex = false;
	
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

//		System.out.println(tables);
	Table createdTable = new Table(strTableName,strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
		this.tables.add(createdTable);
		writeTables(tables);
		
	}
	public void writeTables(Vector<Table> tmpTables) {

		try {
			FileOutputStream fileOut = new FileOutputStream("./src/main/resources/data/" + "tablesArray" + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(tmpTables);
			out.close();
			fileOut.close();
//			System.out.println("Serialized data is saved in " + strTableName + " P" + indicator + ".ser");
		} catch (IOException e) {
//			e.printStackTrace();
		}
	}
	public static Vector<Table> readTables() {

		try {
			FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + "tablesArray" + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Vector<Table> e = (Vector<Table>) in.readObject();
//			System.out.println(e);
		

			in.close();
			fileIn.close();
			return e;
		} catch (IOException i) {
//			i.printStackTrace();
			return null;

		} catch (ClassNotFoundException c) {
			System.out.println("Page class not found");
//			c.printStackTrace();
			return null;
		}
	}
	
	public void writeIndices(Vector<GridIndex> temp) {

		try {
//			System.out.println(temp);
			for(int i=0;i<temp.size();i++) {
//			System.out.println(temp.get(i).getname());
			}
			FileOutputStream fileOut = new FileOutputStream("./src/main/resources/data/" + "GridIndex" + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(temp);
			out.close();
			fileOut.close();
//			System.out.println("Serialized data is saved in " + strTableName + " P" + indicator + ".ser");
		} catch (IOException e) {
//			e.printStackTrace();
		}
	}	
public void writePage(Page page, int indicator,String strTableName) {
		try {
			FileOutputStream fileOut = new FileOutputStream("./src/main/resources/data/" + strTableName + "P" + indicator + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
//			System.out.println("Serialized data is saved in " + strTableName + " P" + indicator + ".ser");
		} catch (IOException e) {
//			e.printStackTrace();
		}
	}
	public Vector<GridIndex> readIndices() {

		try {
			FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + "GridIndex" + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Vector<GridIndex> e = (Vector<GridIndex>) in.readObject();
//			System.out.println(e);
//			System.out.println("What I read"+e);

			in.close();
			fileIn.close();
			return e;
		} catch (IOException i) {
//			i.printStackTrace();
			return null;
		
		} catch (ClassNotFoundException c) {
			System.out.println("Page class not found");
//			c.printStackTrace();
			return null;
		}
	}
	
	@Override
	public void createIndex(String strTableName, String[] strarrColumnNames) throws DBAppException {
		tables = readTables();
		
		Table temp =null;
		Boolean flag = false;
		for(int i=0;i<tables.size();i++) {
//			System.out.println(tables.get(i).getName());
			if(tables.get(i).getName().equals(strTableName)) {
				temp=tables.get(i);
				flag = true;
			}
		}
		if(! flag) {
			System.out.println("table not found. cant create index");
			throw new DBAppException();
			
		}
		Vector<String> vectColumnNames = new Vector<String>();
		for(int i=0;i<strarrColumnNames.length;i++) {
			vectColumnNames.add(strarrColumnNames[i]);
		}
//		this.indices= readIndices();
		GridIndex grid = new GridIndex(temp, vectColumnNames);
//		System.out.println(insertedAnIndex);
		if(!insertedAnIndex) {
			this.insertedAnIndex = true;
			indices.add(grid);
			this.writeIndices(indices);
		}else {
//			System.out.println("dakhalt");
			this.indices= readIndices();
			indices.add(grid);
			this.writeIndices(indices);
		}

//		indices.add(grid);

//		this.writeIndices(indices);
		
		
		Vector <Integer > p =temp.getPages();
		for(int i =0;i<p.size();i++) {
		Page pagetemp = temp.readPage(i, temp.getName());
		Vector <Tuple> tuples = pagetemp.readTuples();
		for(int j =0;j<tuples.size();j++) {
//			System.out.println("inserting in grid  "+strTableName);
			
			this.insertintogrid(tuples.get(j), strTableName, i,indices);
//			System.out.println("inserted in grid " +strTableName);
		}
		
		}
		
	}

	@Override
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables = readTables();		
		
		
		
		Collection<Object> Values= htblColNameValue.values();
		Vector<Object> Values2 = new Vector<Object>();
//		System.out.println(Values);
		Collection<String> Namess= htblColNameValue.keySet();
		Vector<String> Namess2 = new Vector<String>();
	
		Vector<String> Types = new Vector<String>();
//		Values.add(ColNames.)
		Table temp = null;
		for(Table table: tables) {
			if(table.getName().equals(strTableName)) {
				temp = table;
			}
		}
		if(temp==null) {
			throw new DBAppException();
		}
		Collection<String>Typess = temp.getHtblColNameType().values();
//		System.out.println(Typess);
		for(String type: Typess) {
			Types.add(type);
		}
		for(Object value: Values) {
			Values2.add(value);
		}
		for(String name: Namess) {
			Namess2.add(name);
			
		}
		for(int i=0;i<Namess2.size();i++) {
		Object min=	temp.getcolmin(Namess2.get(i));
		Object max=	temp.getcolmax(Namess2.get(i));
		
		Object value = Values2.get(i);
		String currType = value.getClass().getName();
//		System.out.println(Values2);
//		System.out.println(Namess2);
//		System.out.println(Types);
		if(currType.equals("java.lang.Integer")) {
			if(max == null|| min == null) {
				throw new DBAppException();
			}
//			System.out.println(value+ " " + max + " "+ min);
			if((Integer)value> Integer.parseInt((String) max) ||(Integer)value< Integer.parseInt((String) min) ) {
				throw new DBAppException();
			}
		}else
		if(currType.equals("java.lang.Double")) {
			if(max == null|| min == null) {
				throw new DBAppException();
			}
			if((Double)value> (Double.parseDouble((String) max)) ||(Double)value< (Double.parseDouble((String)min ))) {
				throw new DBAppException();
			}
		}else
		if(currType.equals("java.lang.String")) {
			if(max == null|| min == null) {
				throw new DBAppException();
			}
//			System.out.println(value + "  " + max + " "+ min);
			if(((String)value).compareTo(((String)max))>0 ||((String)value).compareTo((String)min) <0) {
				throw new DBAppException();
			}
		}else {
			//Ha-insert hena hwar el date 
		}
		
		
		
		}
//		System.out.println(htblColNameValue);
		
		
		
		
		
		
		ArrayList<String> tmpNames = new ArrayList<String>();
		for(Table table:tables) {
		
			tmpNames.add(table.getName());
			if(table.getName().equals(strTableName)) {
				String key = table.getTableKey();
				Set<String>	names= table.getHtblColNameType().keySet();
				Set<String>	insertNames =  htblColNameValue.keySet();
//				System.out.println(names);
//				System.out.println(insertNames);
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
				
				
				
//				System.out.println("inserting into table");
				int p =table.insertSortedTuple(htblColNameValue, strTableName,key);
//				System.out.println("insert complete");
				Vector<String> name2 = new Vector<String>();
				Vector<Object> obj = new Vector<Object>();
				Set<String> enu = htblColNameValue.keySet();
				for(String enus: enu)
				{
					name2.add(enus);
				}
				for(int i =0;i<name2.size();i++) {
					obj.add(htblColNameValue.get(name2.get(i)));
				}
				Tuple t = new Tuple (obj,0,name2);
//				System.out.println("i am here1");
				if(!this.indices.isEmpty()) {
//					System.out.println("i am here");
				if(checkindexexists(strTableName)) {
//					System.out.println("testing");
					this.insertintogrid(t, strTableName, p,indices);
				}
				return;
			}
			}
		}
		if(!tmpNames.contains(strTableName)) {
			throw new DBAppException();
		}
		
		
		
//		throw new DBAppException("Table not found");
		
		
		

	}
	
	public void insertintogrid (Tuple t,String tablename,int p,Vector<GridIndex> indices2) {
		Vector<Integer> tempBuckets = new Vector<Integer>();
		if(!indices.isEmpty()) {
		indices = indices2;
//		System.out.println("Da el indices"+indices);
		for (int i =0;i<indices2.size();i++) {
			if(indices2.get(i).getname().equals(tablename)) {
				tempBuckets = indices2.get(i).insertintogrid(t,p);
//				System.out.println("Da el temp Buckets "+ tempBuckets);
				indices2.get(i).setBuckets(tempBuckets);
			}
		}
		}
		this.writeIndices(indices2);
	}
	private boolean checkindexexists(String strTableName) {
//		System.out.println("checking index");
		indices = readIndices();
		ArrayList<String> tmpNames = new ArrayList<String>();
		for(GridIndex idex:indices) {
		
			tmpNames.add(idex.getname());
			if(idex.getname().equals(strTableName)) {
				return true;
			}
		
		}
		
		
		
		return false;
	}

	@Override
	public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		indices = readIndices();
		tables = readTables();
		Table tempTable = null;
		GridIndex tempGrid = null;
		if(indices!=null) {
			

			
			ArrayList<String> tmpNames = new ArrayList<String>();
			for(Table table:tables) {
				if(table.getName().equals(strTableName)) {
					String key = table.getTableKey();
					if(table.getName().equals(strTableName)) {
						Set<String>	names= table.getHtblColNameType().keySet();
						Set<String>	insertNames =  htblColNameValue.keySet();
//						System.out.println(names);
//						System.out.println(insertNames);
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
//						table.deleteTuple(htblColNameValue, strTableName, key);
					}
				}
				tmpNames.add(table.getName());
			}
			if(!tmpNames.contains(strTableName)) {
				throw new DBAppException();
			}	
			
			
			
			
		if(checkindexexists(strTableName)) {
			for(int i=0;i<indices.size();i++) {
				if(indices.get(i).getname().equals(strTableName)) {
					tempGrid = indices.get(i);
					break;
				}
			}
			
			
			for(Table tu:tables) {
				if(tu.getName().equals(strTableName)) {
					tempTable = tu;
				}
			}
//			System.out.println(tempTable.getName() + " Da el current table now");
			if(tempTable==null) {
				return;
			}
//			System.out.println(tempTable.getHtblColNameType());
			String clusteringCol = tempTable.getTableKey();
///			keda m3aya el grid fel tempGrid
//			
			Set<String> tempNames = htblColNameValue.keySet();
			System.out.println(tempNames);
			Collection<Object> tempValues = htblColNameValue.values();
			Vector<String> colNamesGrid = tempGrid.getColnames();
			Vector<String> tempNames3 = new Vector<String>();
			Vector<Object> tempValues3 = new Vector<Object>();
			Vector<Object> tempValues4 = new Vector<Object>();		
			tempNames3.addAll(tempNames);
			tempValues3.addAll(tempValues);
			for(int i=0;i<tempValues3.size();i++) {
//				System.out.println("Da el clustering Col "+clusteringCol);
//				System.out.println(tempNames3.get(i));
				if(tempNames3.get(i).equals(clusteringCol)) {

					switch(strClusteringKeyValue.getClass().getName()) {
					case "java.lang.Double":tempValues4.add(Double.parseDouble(strClusteringKeyValue));break;
					case "java.lang.Integer":tempValues4.add(Integer.parseInt(strClusteringKeyValue));break;
					case "java.lang.String":tempValues4.add(strClusteringKeyValue);break;
					
					}
				}else {
					tempValues4.add(tempValues3.get(i));
				}
			}
//			System.out.println("Da TempValues3 " + tempValues3);
//			System.out.println("Da TempValues4 " + tempValues4);

			//El columns el ha-index 3aleha
			Vector<Integer> valuesIndex = new Vector<Integer>();
			for(int i=0;i<tempNames3.size();i++) {
				if(colNamesGrid.indexOf(tempNames3.get(i))!=-1){
					valuesIndex.add(i);
				}
			}
			
			
			Vector<Object> objectToFindIndex = new Vector<Object>();
			for(int i=0;i<valuesIndex.size();i++)
			objectToFindIndex.add(tempValues3.get(valuesIndex.get(i)));
			
			
			
			int[] index = new int[colNamesGrid.size()];
			for(int i=0;i<objectToFindIndex.size();i++)
			index[i] = tempGrid.getplace(objectToFindIndex.get(i), i);
			
			
			//el bucket el fe el values el matching
			
			int BucketNo = tempGrid.getindex(index);
			Bucket BucketToUpdateFrom = tempGrid.readBucket(BucketNo, strTableName);
//			System.out.println("Da el bucket number "+ BucketNo);
			Vector<Object> tempLocations = BucketToUpdateFrom.readlocation();
			Vector<Tuple> SusTuples = new Vector<Tuple>();
			for(int i=0;i<tempLocations.size();i++) {
				Object temp = tempLocations.get(i);
				Vector<Integer> temp2 = (Vector<Integer>) temp;
				SusTuples.add(readTupleWithIndex(temp2.get(0),temp2.get(1),strTableName));
			}
			int temp = 0;
			for(int i=0;i<SusTuples.size();i++) {
				Vector<Object> susAttrs = SusTuples.get(i).getAttributes();
				if(susAttrs == tempValues3) {
					temp = i;
				}
				
			}
			
			
			//keda 3amalt update fe el page
			
			Vector<Integer> pageTuplePair =(Vector<Integer>) tempLocations.get(temp);
			Page tempPage = tempTable.readPage(pageTuplePair.get(0), strTableName);
			Vector<Tuple> tuplesInPage = tempPage.readTuples();
			Tuple tempUpdTup = tuplesInPage.get(pageTuplePair.get(1));
			tempUpdTup.setAttributes(tempValues4);
			tuplesInPage.set(pageTuplePair.get(1), tempUpdTup);
			tempPage.writeTuples(tuplesInPage);
			tempTable.writePage(tempPage,pageTuplePair.get(0),strTableName);
			
			//keda update fel grid
//			tempLocations.remove(temp);
			BucketToUpdateFrom.writeLocation(tempLocations);
			tempGrid.writeBucket(BucketToUpdateFrom, BucketNo, strTableName);
			

//			tempGrid.getplace(object, n);
			
		}
		}
		else {
		ArrayList<String> tmpNames = new ArrayList<String>();
		for(Table table:tables) {
			
			String key = table.getTableKey();
			if(table.getName().equals(strTableName)){
				Set<String>	names= table.getHtblColNameType().keySet();
				Set<String>	insertNames =  htblColNameValue.keySet();
//				System.out.println(names);
//				System.out.println(insertNames);
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
				int x=table.updateTuple(strClusteringKeyValue,htblColNameValue,strTableName,key);
			
				return;
			}
			tmpNames.add(table.getName());
		}

		if(!tmpNames.contains(strTableName)) {
			throw new DBAppException();
		}
		
		}
		
	}
	public void updateGridIndex (String tablename,Tuple tuple,int p) {
		
	}
	@Override
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables = readTables();
		indices= readIndices();
		Table tempTable = null;
		GridIndex tempGrid = null;
		if(indices!=null) {
			
			
			ArrayList<String> tmpNames = new ArrayList<String>();
			for(Table table:tables) {
				if(table.getName().equals(strTableName)) {
					String key = table.getTableKey();
					if(table.getName().equals(strTableName)) {
						Set<String>	names= table.getHtblColNameType().keySet();
						Set<String>	insertNames =  htblColNameValue.keySet();
//						System.out.println(names);
//						System.out.println(insertNames);
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
//						table.deleteTuple(htblColNameValue, strTableName, key);
					}
				}
				tmpNames.add(table.getName());
			}
			if(!tmpNames.contains(strTableName)) {
				throw new DBAppException();
			}	
			
			
			
			
			
			
			
			
			
			
			
			
			
		if(checkindexexists(strTableName)) {
			for(int i=0;i<indices.size();i++) {
				if(indices.get(i).getname().equals(strTableName)) {
					tempGrid = indices.get(i);
					break;
				}
			}
			
			
			for(Table tu:tables) {
				if(tu.getName().equals(strTableName)) {
					tempTable = tu;
				}
			}
			if(tempTable==null) {
				return;
//				throw new DBAppException("TableNotFound");
			}
			
//			keda m3aya el grid fel tempGrid
//			
			Set<String> tempNames = htblColNameValue.keySet();
			Collection<Object> tempValues = htblColNameValue.values();
			Vector<String> colNamesGrid = tempGrid.getColnames();
			Vector<String> tempNames3 = new Vector<String>();
			Vector<Object> tempValues3 = new Vector<Object>();
			tempNames3.addAll(tempNames);
			tempValues3.addAll(tempValues);
			Vector<Integer> valuesIndex = new Vector<Integer>();
			for(int i=0;i<tempNames3.size();i++) {
				if(colNamesGrid.indexOf(tempNames3.get(i))!=-1){
					valuesIndex.add(i);
				}
			}
			Vector<Object> objectToFindIndex = new Vector<Object>();
			for(int i=0;i<valuesIndex.size();i++)
			objectToFindIndex.add(tempValues3.get(valuesIndex.get(i)));
			
			int[] index = new int[colNamesGrid.size()];
			for(int i=0;i<objectToFindIndex.size();i++)
			index[i] = tempGrid.getplace(objectToFindIndex.get(i), i);
			
			int BucketNo = tempGrid.getindex(index);
			Bucket BucketToDeleteFrom = tempGrid.readBucket(BucketNo, strTableName);
//			System.out.println("Current Bucket Number "+BucketNo);
//			System.out.println("Kol el buckets "+tempGrid.getBuckets());
			Vector<Object> tempLocations = BucketToDeleteFrom.readlocation();
			Object locationToDelete = null;
			Vector<Tuple> SusTuples = new Vector<Tuple>();
			for(int i=0;i<tempLocations.size();i++) {
				Object temp = tempLocations.get(i);
				Vector<Integer> temp2 = (Vector<Integer>) temp;
				SusTuples.add(readTupleWithIndex(temp2.get(0),temp2.get(1),strTableName));
			}
			int temp = 0;
			for(int i=0;i<SusTuples.size();i++) {
				Vector<Object> susAttrs = SusTuples.get(i).getAttributes();
				if(susAttrs == tempValues3) {
					temp = i;
				}
				
			}
			//keda 3amalt delete mn el page
			@SuppressWarnings("unchecked")
			Vector<Integer> pageTuplePair =(Vector<Integer>) tempLocations.get(temp);
			Page tempPage = tempTable.readPage(pageTuplePair.get(0), strTableName);
			Vector<Tuple> tuplesInPage = tempPage.readTuples();
			int TupleNo= pageTuplePair.get(1);
//			System.out.println("Size abl el deletion "+tuplesInPage.size());
			tuplesInPage.remove(TupleNo);
			for(int i=TupleNo;i<tuplesInPage.size();i++) {
				int tempTempLoc = tuplesInPage.get(i).getLocInPage()-1;
				tuplesInPage.get(i).setLocInPage(tempTempLoc);
			}
//			System.out.println("Size ba3d el deletion "+tuplesInPage.size());
			tempPage.writeTuples(tuplesInPage);
			tempTable.writePage(tempPage,pageTuplePair.get(0),strTableName);
			
//			System.out.println("Abl el remove locaiton"+tempLocations.size());
			tempLocations.remove(temp);
//			System.out.println("Ba3d el remove locaiton"+tempLocations.size());
			BucketToDeleteFrom.writeLocation(tempLocations);
			tempGrid.writeBucket(BucketToDeleteFrom, BucketNo, strTableName);
			

//			tempGrid.getplace(object, n);
			
		}
		}
		else {
		
		ArrayList<String> tmpNames = new ArrayList<String>();
		for(Table table:tables) {
			if(table.getName().equals(strTableName)) {
				String key = table.getTableKey();
				if(table.getName().equals(strTableName)) {
					Set<String>	names= table.getHtblColNameType().keySet();
					Set<String>	insertNames =  htblColNameValue.keySet();
//					System.out.println(names);
//					System.out.println(insertNames);
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
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		this.tables = readTables();
		Vector<String> tableNames = new Vector<String>();
		for(int i=0;i<this.tables.size();i++) {
			tableNames.add(this.tables.get(i).getName());
		}
		for(int i=0;i<sqlTerms.length;i++) {
			if(tableNames.indexOf(sqlTerms[i]._strTableName)==-1) {
				throw new DBAppException();
			}
		}
		Vector<Tuple> finalQuery = new Vector<Tuple>();
		Vector<Tuple> finalQueryIndexed = new Vector<Tuple>();
		Vector<Tuple> queryResult = new Vector<Tuple>();
		for(int sql=0;sql<sqlTerms.length;sql++) {
			if(sql==0) {
	//=============***** CODE BLOCK OF AND OPERATOR ******===============	
		if(arrayOperators[sql].equals("AND")) 
		{
		
			
			
			
			String tableName2 = sqlTerms[sql+1]._strTableName;
			String colName2 = sqlTerms[sql+1]._strColumnName;
			String operator2 = sqlTerms[sql+1]._strOperator;
			Object value2 = sqlTerms[sql+1]._objValue;
			queryResult.removeAllElements();
			String tableName = sqlTerms[sql]._strTableName;
			String colName = sqlTerms[sql]._strColumnName;
			String operator = sqlTerms[sql]._strOperator;
			Object value = sqlTerms[sql]._objValue;
			Table currentTable =null;
			Table currentTable2=null;
			Vector<Tuple> queryResult2 = new Vector<Tuple>();
			tables = readTables();
			for(int i=0;i<tables.size();i++) {
				if(tables.get(i).getName().equals(tableName)) {
					if(currentTable==null){
					currentTable=tables.get(i);
					}else {
					}
				}
				if(tables.get(i).getName().equals(tableName2)) {
					if(currentTable2==null){
					currentTable2=tables.get(i);
					}else {
					}
				}
				}
//			System.out.println("Abl ma a-check");
//			System.out.println(tableName);
//			indices=readIndices();
//			for(int h=0;h<indices.size();h++){
//				System.out.println("Entafeet  "+indices.get(h).getname());
//			}
//			System.out.println(checkindexexists(tableName));
			GridIndex currQueryGrid = null;
			if(checkindexexists(tableName)) {
//				indices = readIndices();
//				System.out.println(indices)
//				System.out.println("Abl ma a-check + firstBreakpoint");
				for(int i=0;i<indices.size();i++) {
					boolean foundFirst = false;
					boolean foundSecond= false;
					if(indices.get(i).getname().equals(tableName)) {
						for(int j=0;j<indices.get(i).getColnames().size();j++) {
							if(indices.get(i).getColnames().get(j).equals(colName)) {
								foundFirst = true;
							}
							if(indices.get(i).getColnames().get(j).equals(colName2)) {
								foundSecond = true;
							}
						
						}
						if(foundFirst&&foundSecond) {
							currQueryGrid = indices.get(i);
							System.out.println("Marwan Pablo  "+currQueryGrid.getBuckets());
							break;
						}
					}
					if(foundFirst&&foundSecond) {
						break;
					}
					
//				System.out.println("De el indices "+indices.get(i).getColnames());
//				System.out.println("De el indices "+indices.get(i).getBuckets());
				

				}
				if(currQueryGrid!=null) {
				int colNumToBePassedToGetPlaceMethod = -1;
				for(int i=0;i<currQueryGrid.getColnames().size();i++) {
					if(currQueryGrid.getColnames().get(i).equals(colName)) {
						colNumToBePassedToGetPlaceMethod = i;
//						System.out.println(colName);
//						System.out.println(i);
					}
						
				}
				int temp1 = -1;
				temp1 = currQueryGrid.getplace(value, colNumToBePassedToGetPlaceMethod);
				int[]indexFinal1 = new int[2];
				indexFinal1[0] = temp1;
				
				
				
				int colNumToBePassedToGetPlaceMethod2 = -1;
				for(int i=0;i<currQueryGrid.getColnames().size();i++) {
					if(currQueryGrid.getColnames().get(i).equals(colName2)) {
//						System.out.println("")
//						System.out.println(colName2);
						
						colNumToBePassedToGetPlaceMethod2 = i;
					}
						
				}
				int temp2 = -1;
				temp2 = currQueryGrid.getplace(value2, colNumToBePassedToGetPlaceMethod2);
//				int[]indexFinal1 = new int[2];
//				System.out.println(temp2);
				indexFinal1[1] = temp2;
				
//				System.out.println(currQueryGrid.getindex(indexFinal1));
//				System.out.println(indexFinal1[0]);
//				System.out.println(indexFinal1[1]);
				
				//Hena m3aya el int[] index beta3 el query value 
				Vector<Integer>Final1 = new Vector<Integer>();
				Final1 = helperForSelectByIndex(indexFinal1[0],operator);
				Vector<Integer>Final2 = new Vector<Integer>();
				Final2 = helperForSelectByIndex(indexFinal1[1],operator2);
				Vector<Integer>finalBuckets = new Vector<Integer>();
//				System.out.println("Da first operator "+operator);
//				System.out.println("Da second operator "+operator2);
//				
//				System.out.println("Da Final 1"+Final1);
//				System.out.println("Da Final 2"+Final2);
				for(int i=0;i<Final2.size();i++) {
					for(int j=0;j<Final1.size();j++) {
						int x = Final1.get(j)*10 + Final2.get(i);
						finalBuckets.add(x);
					}
				}
//				System.out.println(finalBuckets);
				
				
				
				}
				
			}

			if(currentTable==null) {break;}
			Vector <Integer> p = currentTable.getPages();
//			p.add(1);
//			System.out.println(p);
			for(int k =0;k<p.size();k++) {
			Page pagetemp = currentTable.readPage(k, currentTable.getName());
//			System.out.println(pagetemp);
			Vector <Tuple> tuples = pagetemp.readTuples();
			for(int j =0;j<tuples.size();j++) {
				Tuple tempTuple = tuples.get(j);
				Vector<String> colNamesTuple = tempTuple.getColnames();
				Vector<Object> valuesTuple = tempTuple.getAttributes();
//				System.out.println(colNamesTuple);
//				System.out.println(valuesTuple);
				for(int m=0;m<colNamesTuple.size();m++) {
					if(colNamesTuple.get(m).equals(colName)) {
						helperForSelect(value,operator,valuesTuple,m,queryResult,tempTuple);
					}
				}
			
		}
	}	
//			System.out.println(currentTable2)
			p =currentTable2.getPages();
//			p.add(1);
			for(int k =0;k<p.size();k++) {
			Page pagetemp = currentTable2.readPage(k, currentTable2.getName());
			Vector <Tuple> tuples = pagetemp.readTuples();
//			System.out.println(tuples);
			for(int j =0;j<tuples.size();j++) {
				Tuple tempTuple = tuples.get(j);
				Vector<String> colNamesTuple = tempTuple.getColnames();
				Vector<Object> valuesTuple = tempTuple.getAttributes();
				for(int m=0;m<colNamesTuple.size();m++) {
				
					if(colNamesTuple.get(m).equals(colName2)) {
//						System.out.println(operator2);
						helperForSelect(value2,operator2,valuesTuple,m,queryResult2,tempTuple);
					}
				}
			
		}
	}
			for(int lol=0;lol<queryResult.size();lol++) {
				if(queryResult2.indexOf(queryResult.get(lol))==-1) {
					finalQuery.add(queryResult.get(lol));
				}
			}
		}
		//=============***** CODE BLOCK OF OR OPERATOR ******===============	
		else if(arrayOperators[sql].equals("OR")){
			String tableName = sqlTerms[sql+1]._strTableName;
			String colName = sqlTerms[sql+1]._strColumnName;
			String operator = sqlTerms[sql+1]._strOperator;
			Object value = sqlTerms[sql+1]._objValue;
			queryResult.removeAllElements();
			String tableName2 = sqlTerms[sql]._strTableName;
			String colName2 = sqlTerms[sql]._strColumnName;
			String operator2 = sqlTerms[sql]._strOperator;
			Object value2 = sqlTerms[sql]._objValue;
//			System.out.println("Da awel value"+value);
//			System.out.println("Da tany value"+value2);
			Table currentTable =null;
			Table currentTable2=null;
			Vector<Tuple> queryResult2 = new Vector<Tuple>();
			tables = readTables();
			for(int i=0;i<tables.size();i++) {
				if(tables.get(i).getName().equals(tableName)) {
					if(currentTable==null){
					currentTable=tables.get(i);
					}else {
					}
				}
				if(tables.get(i).getName().equals(tableName2)) {
					if(currentTable2==null){
					currentTable2=tables.get(i);
					}else {
					}
				}
				}
//			
//			GridIndex currQueryGrid = null;
//			if(checkindexexists(tableName)) {
////				indices = readIndices();
////				System.out.println(indices)
////				System.out.println("Abl ma a-check + firstBreakpoint");
//				for(int i=0;i<indices.size();i++) {
//					boolean foundFirst = false;
//					boolean foundSecond= false;
//					if(indices.get(i).getname().equals(tableName)) {
//						for(int j=0;j<indices.get(i).getColnames().size();j++) {
//							if(indices.get(i).getColnames().get(j).equals(colName)) {
//								foundFirst = true;
//							}
//							if(indices.get(i).getColnames().get(j).equals(colName2)) {
//								foundSecond = true;
//							}
//						
//						}
//						if(foundFirst&&foundSecond) {
//							currQueryGrid = indices.get(i);
//							System.out.println("Marwan Pablo  "+currQueryGrid.getBuckets());
//							break;
//						}
//					}
//					if(foundFirst&&foundSecond) {
//						break;
//					}
//					
////				System.out.println("De el indices "+currQueryGrid.getColnames());
////				System.out.println("De el indices "+indices.get(i).getBuckets());
//				
//
//				}
//				if(currQueryGrid!=null) {
//					System.out.println("De el indices "+currQueryGrid.getColnames());
//				int colNumToBePassedToGetPlaceMethod = -1;
//				for(int i=0;i<currQueryGrid.getColnames().size();i++) {
//					if(currQueryGrid.getColnames().get(i).equals(colName)) {
//						colNumToBePassedToGetPlaceMethod = i;
//						
////						System.out.println(colName);
//						System.out.println("Awel wahed"+i);
//					}
//						
//				}
//				int temp1 = -1;
//				temp1 = currQueryGrid.getplace(value, colNumToBePassedToGetPlaceMethod);
//				int[]indexFinal1 = new int[2];
//				indexFinal1[0] = temp1;
//				
//				
//				
//				int colNumToBePassedToGetPlaceMethod2 = -1;
//				for(int i=0;i<currQueryGrid.getColnames().size();i++) {
//					if(currQueryGrid.getColnames().get(i).equals(colName2)) {
//						System.out.println("Tany wahed "+i);
////						System.out.println(colName2);
//						
//						colNumToBePassedToGetPlaceMethod2 = i;
//					}
//						
//				}
//				int temp2 = -1;
//				temp2 = currQueryGrid.getplace(value2, colNumToBePassedToGetPlaceMethod2);
////				int[]indexFinal1 = new int[2];
////				System.out.println(temp2);
//				indexFinal1[1] = temp2;
//				
////				System.out.println(currQueryGrid.getindex(indexFinal1));
////				System.out.println(indexFinal1[0]);
////				System.out.println(indexFinal1[1]);
//				
//				//Hena m3aya el int[] index beta3 el query value 
//				Vector<Integer>Final1 = new Vector<Integer>();
//				Final1 = helperForSelectByIndex(indexFinal1[0],operator);
//				Vector<Integer>Final2 = new Vector<Integer>();
//				Final2 = helperForSelectByIndex(indexFinal1[1],operator2);
//				Vector<Integer>finalBuckets = new Vector<Integer>();
////				System.out.println("Da first operator "+operator);
////				System.out.println("Da second operator "+operator2);
//				
//				System.out.println("Da Final 1"+Final1);
//				System.out.println("Da Final 2"+Final2);
////				for(int i=0;i<Final2.size();i++) {
////					for(int j=0;j<10;j++) {
////						int x = Final2.get(i)*10 + j;
////						if(finalBuckets.indexOf(x)==-1)
////						finalBuckets.add(x);
////					}
////				}
////				for(int i=0;i<Final1.size();i++) {
////					for(int j=0;j<10;j++) {
////						int x = Final1.get(i) + j*10 ;
////						if(finalBuckets.indexOf(x)==-1)
////						finalBuckets.add(x);
////					}
////				}
//				finalBuckets.removeAllElements();
////				for(int i=43;i<100;i++) {
////					finalBuckets.add(i);
////				}
////				System.out.println("Current Grid Buckets "+currQueryGrid.getBuckets());
////				System.out.println("Final Buckets "+finalBuckets);
//				Vector<Integer> trial = new Vector<Integer>();
//				trial.add(44);
//				trial.add(11);
//				trial.add(22);
//				trial.add(33);
//				trial.add(55);
////				trial.add(66);
////				trial.add(77);
////				trial.add(88);
////				trial.add(99);	
//				trial.add(0);
//				for(int i=0;i<trial.size();i++) {
//					if(true) {
//						Bucket temp = currQueryGrid.readBucket(trial.get(i), currQueryGrid.getname());
//						Vector<Object> tempLocation = temp.readlocation();
////						System.out.println("Temp Location "+tempLocation);
//						for(int k=0;k<tempLocation.size();k++) {
//							Vector<Integer> x = (Vector<Integer>) tempLocation.get(k);
////							System.out.println("PageNoTupleNoPairs "+x);
//							Tuple indexInsertion = readTupleWithIndex(x.get(0),x.get(1),currQueryGrid.getname());
////							System.out.println("Tuple to be inserted"+indexInsertion);
//							if(trial.get(i)==55) {
//							Vector<Object>attrs = indexInsertion.getAttributes();
//							Vector<String>colNames = indexInsertion.getColnames();
//							int tempss=-1;
//							for(int l=0;l<colNames.size();l++) {
//								if(colNames.get(l).equals(colName)) {
//									tempss = l;
//									break;
//								}
//							}
////							System.out.println("Da values el tempss "+attrs.get(tempss));
//							double temp123 = (Double.parseDouble((String) attrs.get(tempss)));
//							double temp456 = Double.parseDouble(value.toString());
//							if(temp123>temp456) {
//								continue;
//							}
//							
//							
//							}
//							finalQueryIndexed.add(indexInsertion);
//						}
//						//tempLocation is The page Tuple pairs I need to Access
//					}
//				}
//				
////				System.out.println(finalBuckets);
//				
////				for(int i=0;i<finalQueryIndexed.size();i++) {
////					if(finalQueryIndexed.lastIndexOf(finalQueryIndexed.get(i))!=i) {
////						finalQueryIndexed.remove(finalQueryIndexed.lastIndexOf(finalQueryIndexed.get(i)));
////					}
////				}
//				System.out.println(finalQueryIndexed.size());
//				
//				}else {
//					System.out.println("Mafes Grid");
//				}
//				
//			}
			if(currentTable==null) {break;}
			Vector <Integer> p = currentTable2.getPages();
			
			for(int k =0;k<p.size();k++) {
			Page pagetemp = currentTable.readPage(k, currentTable.getName());
			Vector <Tuple> tuples = pagetemp.readTuples();
//			System.out.println("Da tuple size "+k+"  "+ tuples.size()+ p);
			for(int j =0;j<tuples.size();j++) {
				Tuple tempTuple = tuples.get(j);
				Vector<String> colNamesTuple = tempTuple.getColnames();
				Vector<Object> valuesTuple = tempTuple.getAttributes();
				for(int m=0;m<colNamesTuple.size();m++) {
					if(colNamesTuple.get(m).equals(colName)) {
						helperForSelect(value,operator,valuesTuple,m,queryResult,tempTuple);
					}
				}
			
		}
	}	
			p =currentTable2.getPages();
			for(int k =0;k<p.size();k++) {
			Page pagetemp = currentTable2.readPage(k, currentTable2.getName());
			Vector <Tuple> tuples = pagetemp.readTuples();
			for(int j =0;j<tuples.size();j++) {
				Tuple tempTuple = tuples.get(j);
				Vector<String> colNamesTuple = tempTuple.getColnames();
				Vector<Object> valuesTuple = tempTuple.getAttributes();
				for(int m=0;m<colNamesTuple.size();m++) {
					if(colNamesTuple.get(m).equals(colName2)) {
						helperForSelect(value2,operator2,valuesTuple,m,queryResult2,tempTuple);
					}
				}
			
		}
	}	

//			System.out.println("Query res1"+queryResult);
//			System.out.println("Query res2"+queryResult2);
			for(int lol=0;lol<queryResult.size();lol++) {	 
				finalQuery.add(queryResult.get(lol));
			
		}
		for(int lol=0;lol<queryResult2.size();lol++) {
			if(queryResult2.indexOf(queryResult.get(lol))!=-1) 
			finalQuery.add(queryResult2.get(lol));
			
	}

//		Vector<Tuple> test = new Vector<Tuple>();
//		for(int i=0;i<finalQuery.size();i++) {
//			Tuple tempTuple = finalQuery.get(i);
//			for(int j=0;j<finalQuery.size();j++) {
//				if(i!=j) {
//					if(finalQuery.get(j).) {
//						finalQuery.remove(j);
//					}
//				}
//			}
//		}

//		System.out.println(test);
//		System.out.println(" Yalla beena"+test.size());

//		for(int i=0;i<finalQuery.size();i++) {
////			System.out.println(finalQuery.get(i));
//			}
//		System.out.println(value);
//		System.out.println(value2);
		}else 
			//=============***** CODE BLOCK OF XOR OPERATOR ******===============	
		{
			String tableName2 = sqlTerms[sql+1]._strTableName;
			String colName2 = sqlTerms[sql+1]._strColumnName;
			String operator2 = sqlTerms[sql+1]._strOperator;
			Object value2 = sqlTerms[sql+1]._objValue;
			queryResult.removeAllElements();
			String tableName = sqlTerms[sql]._strTableName;
			String colName = sqlTerms[sql]._strColumnName;
			String operator = sqlTerms[sql]._strOperator;
			Object value = sqlTerms[sql]._objValue;
//			System.out.println("Da awel value"+value);
//			System.out.println("Da tany value"+value2);
			Table currentTable =null;
			Table currentTable2=null;
			Vector<Tuple> queryResult2 = new Vector<Tuple>();
			tables = readTables();
			for(int i=0;i<tables.size();i++) {
				if(tables.get(i).getName().equals(tableName)) {
					if(currentTable==null){
					currentTable=tables.get(i);
					}else {
					}
				}
				if(tables.get(i).getName().equals(tableName2)) {
					if(currentTable2==null){
					currentTable2=tables.get(i);
					}else {
					}
				}
				}
			if(currentTable==null) {break;}
			Vector <Integer> p = currentTable2.getPages();
			
			for(int k =0;k<p.size();k++) {
			Page pagetemp = currentTable.readPage(k, currentTable.getName());
			Vector <Tuple> tuples = pagetemp.readTuples();
//			System.out.println("Da tuple size "+k+"  "+ tuples.size()+ p);
			for(int j =0;j<tuples.size();j++) {
				Tuple tempTuple = tuples.get(j);
				Vector<String> colNamesTuple = tempTuple.getColnames();
				Vector<Object> valuesTuple = tempTuple.getAttributes();
				for(int m=0;m<colNamesTuple.size();m++) {
					if(colNamesTuple.get(m).equals(colName)) {
						helperForSelect(value,operator,valuesTuple,m,queryResult,tempTuple);
					}
				}
			
		}
	}	
			p =currentTable2.getPages();
			for(int k =0;k<p.size();k++) {
			Page pagetemp = currentTable2.readPage(k, currentTable2.getName());
			Vector <Tuple> tuples = pagetemp.readTuples();
			for(int j =0;j<tuples.size();j++) {
				Tuple tempTuple = tuples.get(j);
				Vector<String> colNamesTuple = tempTuple.getColnames();
				Vector<Object> valuesTuple = tempTuple.getAttributes();
				for(int m=0;m<colNamesTuple.size();m++) {
					if(colNamesTuple.get(m).equals(colName2)) {
						helperForSelect(value2,operator2,valuesTuple,m,queryResult2,tempTuple);
					}
				}
			
		}
	}
			for(int lol=0;lol<queryResult.size();lol++) {
				if(queryResult.indexOf(queryResult2.get(lol))==-1) {
					finalQuery.add(queryResult.get(lol));
				}
			}
			for(int lol=0;lol<queryResult.size();lol++) {
				if(queryResult2.indexOf(queryResult.get(lol))==-1) {
					finalQuery.add(queryResult2.get(lol));
				}
			}
		}
		sql++;
			}else {
				if(arrayOperators[sql-1].equals("AND")) {
					queryResult.removeAllElements();
					String tableName = sqlTerms[sql]._strTableName;
					String colName = sqlTerms[sql]._strColumnName;
					String operator = sqlTerms[sql]._strOperator;
					Object value = sqlTerms[sql]._objValue;
					Table currentTable =null;
					for(int i=0;i<tables.size();i++) {
						if(tables.get(i).getName()==tableName) {
							if(currentTable==null){
							currentTable=tables.get(i);
						}
						}
					}
					if(currentTable==null) {
						continue;
					}
					
					Vector <Integer > p =currentTable.getPages();
					for(int k =0;k<p.size();k++) {
					Page pagetemp = currentTable.readPage(k, currentTable.getName());
					Vector <Tuple> tuples = pagetemp.readTuples();
					for(int j =0;j<tuples.size();j++) {
						Tuple tempTuple = tuples.get(j);
						Vector<String> colNamesTuple = tempTuple.getColnames();
						Vector<Object> valuesTuple = tempTuple.getAttributes();
						for(int m=0;m<colNamesTuple.size();m++) {
							if(colNamesTuple.get(m)==colName) {
								helperForSelect(value,operator,valuesTuple,m,queryResult,tempTuple);
							}
						}
					
				}
			}
					Vector<Tuple> tempo = new Vector<Tuple>();
					for(int v=0;v<queryResult.size();v++) {
						if(finalQuery.indexOf(queryResult.get(v))!=-1) {
							tempo.add(queryResult.get(v));
						}
					}
					finalQuery = tempo;
					
					
				}else if(arrayOperators[sql-1].equals("OR")) {
					queryResult.removeAllElements();
					String tableName = sqlTerms[sql]._strTableName;
					String colName = sqlTerms[sql]._strColumnName;
					String operator = sqlTerms[sql]._strOperator;
					Object value = sqlTerms[sql]._objValue;
					Table currentTable =null;
					for(int i=0;i<tables.size();i++) {
						if(tables.get(i).getName()==tableName) {
							if(currentTable==null){
							currentTable=tables.get(i);
						}
						}
					}
					if(currentTable==null) {
						continue;
					}
					Vector <Integer > p =currentTable.getPages();
					for(int k =0;k<p.size();k++) {
					Page pagetemp = currentTable.readPage(k, currentTable.getName());
					Vector <Tuple> tuples = pagetemp.readTuples();
					for(int j =0;j<tuples.size();j++) {
						Tuple tempTuple = tuples.get(j);
						Vector<String> colNamesTuple = tempTuple.getColnames();
						Vector<Object> valuesTuple = tempTuple.getAttributes();
						for(int m=0;m<colNamesTuple.size();m++) {
							if(colNamesTuple.get(m)==colName) {
								helperForSelect(value,operator,valuesTuple,m,queryResult,tempTuple);
							}
						}
					
				}
			}
				
					for(int v=0;v<queryResult.size();v++) {
						finalQuery.add(queryResult.get(v));
					}
					
				}else {
					queryResult.removeAllElements();
					String tableName = sqlTerms[sql]._strTableName;
					String colName = sqlTerms[sql]._strColumnName;
					String operator = sqlTerms[sql]._strOperator;
					Object value = sqlTerms[sql]._objValue;
					Table currentTable =null;
					for(int i=0;i<tables.size();i++) {
						if(tables.get(i).getName()==tableName) {
							if(currentTable==null){
							currentTable=tables.get(i);
						}
						}
					}
					if(currentTable==null) {
						continue;
					}
					Vector <Integer > p =currentTable.getPages();
					for(int k =0;k<p.size();k++) {
					Page pagetemp = currentTable.readPage(k, currentTable.getName());
					Vector <Tuple> tuples = pagetemp.readTuples();
					for(int j =0;j<tuples.size();j++) {
						Tuple tempTuple = tuples.get(j);
						Vector<String> colNamesTuple = tempTuple.getColnames();
						Vector<Object> valuesTuple = tempTuple.getAttributes();
						for(int m=0;m<colNamesTuple.size();m++) {
							if(colNamesTuple.get(m)==colName) {
								helperForSelect(value,operator,valuesTuple,m,queryResult,tempTuple);
							}
						}
					
				}
			}
				
					Vector<Tuple> tempo = new Vector<Tuple>();
					for(int v=0;v<queryResult.size();v++) {
						if(finalQuery.indexOf(queryResult.get(v))!=-1) {
							tempo.add(queryResult.get(v));
						}
					}
					for(int v=0;v<finalQuery.size();v++) {
						if(queryResult.indexOf(finalQuery.get(v))!=-1) {
							tempo.add(finalQuery.get(v));
						}
					}
					finalQuery = tempo;
				}
			}
			
		
		
		
		}
//		System.out.println(finalQuery);
//		System.out.println(finalQueryIndexed);
//		System.out.println("Final Query Value"+finalQuery);
//		System.out.println(finalQuery.size());
		

		Iterator result = finalQuery.iterator();
//		
		while(result.hasNext())
            System.out.println(result.next());
//		System.out.println("Iterator Value"+result);
		return  result;
	}
	public Vector<Integer> helperForSelectByIndex(int x,String operator){
		Vector<Integer> finale = new Vector<Integer>();
		switch(operator) {
		case "=": finale.add(x);break;
		case "!=": for(int i=0;i<10;i++) {if(i!=x) {finale.add(i);}}break;
		case ">=": for(int i=0;i<=x;i++) { {finale.add(i);}}break;
		case "<=": for(int i=x;i<10;i++) { {finale.add(i);}}break;
		case "<": for(int i=0;i<x;i++) { {finale.add(i);}}break;
		case ">": for(int i=x+1;i<10;i++) { {finale.add(i);}}	break;
		}

		return finale;
	}
	public static void helperForSelect(Object value2, String operator2,Vector<Object>valuesTuple,int m,Vector<Tuple>queryResult2,Tuple tempTuple) {
		if(operator2.equals("=")) {
			switch(value2.getClass().getName()) {
			case "java.lang.Double":{if( (Double.parseDouble( value2.toString()))==(Double.parseDouble(valuesTuple.get(m).toString()))) {queryResult2.add(tempTuple);}}break;
			case "java.lang.String":{if(((String)value2).compareTo((String) valuesTuple.get(m))==0) {queryResult2.add(tempTuple);}}break;
			case "java.lang.Integer":{if((Integer.parseInt((String) value2))==(Integer.parseInt((String)valuesTuple.get(m)))) {queryResult2.add(tempTuple);}}break;
			
			}
		}else if(operator2.equals("!=")) {
			switch(value2.getClass().getName()) {
			case "java.lang.Double":{if((Double.parseDouble( value2.toString()))!=(Double.parseDouble(valuesTuple.get(m).toString()))) {queryResult2.add(tempTuple);}}break;
			case "java.lang.String":{if(((String)value2).compareTo((String) valuesTuple.get(m))!=0) {queryResult2.add(tempTuple);}}break;
			case "java.lang.Integer":{if((Integer.parseInt( value2.toString()))!=(Integer.parseInt((String)valuesTuple.get(m)))) {queryResult2.add(tempTuple);}}break;
			
			}
		}else if(operator2.equals(">=")) {
			switch(value2.getClass().getName()) {
			case "java.lang.Double":{if((Double.parseDouble(value2.toString()))<=(Double.parseDouble(valuesTuple.get(m).toString()))) {queryResult2.add(tempTuple);}}break;
			case "java.lang.String":{if(((String)value2).compareTo((String) valuesTuple.get(m))>=0) {queryResult2.add(tempTuple);}}break;
			case "java.lang.Integer":{if((Integer.parseInt(value2.toString()))<=(Integer.parseInt((String)valuesTuple.get(m)))) {queryResult2.add(tempTuple);}}break;
			
			}
		}else if(operator2.equals("<=")) {
			switch(value2.getClass().getName()) {
			case "java.lang.Double":{if((Double.parseDouble(value2.toString()))>=(Double.parseDouble(valuesTuple.get(m).toString()))) {queryResult2.add(tempTuple);}}break;
			case "java.lang.String":{if(((String)value2).compareTo((String) valuesTuple.get(m))<=0) {queryResult2.add(tempTuple);}}break;
			case "java.lang.Integer":{if((Integer.parseInt((String) value2))>=(Integer.parseInt((String)valuesTuple.get(m)))) {queryResult2.add(tempTuple);}}break;
			
			}
		}else if(operator2.equals(">")) {
			switch(value2.getClass().getName()) {
			case "java.lang.Double":{if((Double.parseDouble(value2.toString()))<(Double.parseDouble(valuesTuple.get(m).toString()))) {queryResult2.add(tempTuple);}}break;
			case "java.lang.String":{if(((String)value2).compareTo((String) valuesTuple.get(m))>0) {queryResult2.add(tempTuple);}}break;
			case "java.lang.Integer":{if((Integer.parseInt(value2.toString()))<(Integer.parseInt((String)valuesTuple.get(m)))) {queryResult2.add(tempTuple);}}break;
			
			}
		}else if(operator2.equals("<")) {
			switch(value2.getClass().getName()) {
			case "java.lang.Double":{if((Double.parseDouble((String) value2))>(Double.parseDouble(valuesTuple.get(m).toString()))) {queryResult2.add(tempTuple);}}break;
			case "java.lang.String":{if(((String)value2).compareTo((String) valuesTuple.get(m))<0) {queryResult2.add(tempTuple);}}break;
			case "java.lang.Integer":{if((Integer.parseInt((String) value2))>(Integer.parseInt((String)valuesTuple.get(m)))) {queryResult2.add(tempTuple);}}break;
			
			}
		}
	}


	public Tuple readTupleWithIndex(int pageNo,int tupleNo,String tableName) {
		Tuple tupleFinal = null;
		Table table = null;
		tables= readTables();
		for(Table temp:tables) {
			if(temp.getName().equals(tableName)) {
				table = temp;
			}
		}
		
		Page page= table.readPage(pageNo,table.getName());
		Vector<Tuple> tempTuples = page.readTuples();
		tupleFinal = tempTuples.get(tupleNo);
		return tupleFinal;
	}
	
	
	
//	@SuppressWarnings("deprecation")
//	public static void main(String[]args) throws DBAppException {
		
//	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}

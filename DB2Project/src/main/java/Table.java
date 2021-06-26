
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Properties;


public class Table implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// private static final long serialVersionUID = 1L;
	 private Vector<Integer> pages = new Vector<Integer>();
	 private Vector<Tuple> tuples = new Vector<Tuple>();
	private String tableName = "";
	private  int maxRows ;
	private int noRows = 0;
	private String tableKey = "";
	private int attrNo = 0;
	private Hashtable<String,String> htblColNameType;
	private Vector<String> columnNames = new Vector<String>();
	private Properties properties = new Properties();
	private Hashtable<String, String> colmin;
	private Hashtable<String, String> colmax;
	
	
	public String getcolmin (String colname) {
		
		return this.colmin.get(colname);
	}
	
	public String getcolmax (String colname) {
		return this.colmax.get(colname);
	}
	
	
	
	public String getcoltype (String colname) {
		return this.htblColNameType.get(colname);
	}
	
	public Table(String strTableName, String strClusteringKey, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {
		this.tableName = strTableName;
		this.tableKey = strClusteringKey;
		this.htblColNameType = htblColNameType;
		this.colmin=htblColNameMin;
		this.colmax=htblColNameMax;
		try {
			this.addToMeta(strClusteringKey, htblColNameType,htblColNameMin,htblColNameMax);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Page first = new Page();
		pages.add(0);
		this.writePage(first, 0,strTableName);
		readFromProps();
	}
	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}
	public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
		this.htblColNameType = htblColNameType;
	}
	//Method that reads the DBApp.config File to get the maxRows and maxKeys
	public void readFromProps() {
		Properties prop = new Properties();
		String fileName = "./src/main/resources/DBApp.config";
		InputStream is = null;
		try {
		    is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {
		    
		}
		try {
		    prop.load(is);
		} catch (IOException ex) {
		    
		}
		int[] a = new int[1];
		//System.out.println(prop.getProperty("MaximumRowsCountinPage"));
		//System.out.println(prop.getProperty("MaximumKeysCountinIndexBucket"));
		String tmpMax=prop.getProperty("MaximumRowsCountinPage");
		maxRows = Integer.parseInt(tmpMax);
//		System.out.println(maxRows);
	}
	//methods that adds to metadata.csv
	public void addToMeta(String key, Hashtable<String, String> table, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws IOException {
		FileWriter writer = new FileWriter(new File("./src/main/resources/metadata.csv"),true);
		try {
			writer.append("Table Name, Column Name, Column Type, ClusteringKey,Indexed,Min,Max ");
			writer.append('\n');
			table.forEach((name, type) -> {
				try {
					columnNames.add(name);
					attrNo++;
					writer.append(this.tableName);
					writer.append(',');
					writer.append(name);
					writer.append(',');
					writer.append(type);
					writer.append(',');
					if (key.equals(name)) {
						tableKey = key;
						writer.append("True");
					} else {
						writer.append("False");
					}
					writer.append(',');
					writer.append("False");
					writer.append(',');
					writer.append(htblColNameMin.get(name));
					writer.append(',');
					writer.append(htblColNameMax.get(name));
					writer.append('\n');

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
//			System.out.println("Meta was created Successfully");
		} finally {
			writer.flush();
			writer.close();
		}
	}

	public String getName() {
		return tableName;

	}

	public Vector<Object> getArrayFromHash(Hashtable<String, Object> hash) {
		Vector<Object> attrs = new Vector<Object>();
		hash.forEach((name, value) -> {
			attrs.add(columnNames.indexOf(name), (Object) value);
		});
		return attrs;
	}

	public int updateTuple(String key, Hashtable<String, Object> htblColNameValue,String strTableName,String clusteringKey) throws DBAppException{
		for (int i = 0; i < pages.size(); i++) {
			Page tempPage = readPage(pages.get(i),strTableName);
			Vector<Tuple> tuples = tempPage.readTuples();
			for (int j = 0; j < tuples.size(); j++) {
				if (((Tuple) tuples.get(j)).getAttributes().contains(key)) {
					Vector<Object> attrs = getArrayFromHash(htblColNameValue);
					Tuple removed = (Tuple) tuples.remove(j);
					removed.setAttributes(attrs);
					this.writePage(tempPage, i,strTableName);
					try {
						insertSortedTuple(htblColNameValue,strTableName,clusteringKey);
					} catch (DBAppException e) {
						System.out.println(e.getMessage());
					}
					readPage(i,strTableName);
					return i;
				}
			}
		}
		return 0;
	}

	public boolean findKey(String key, Page page) {

		for (int i = 0; i < page.readTuples().size(); i++) {
			if (((Tuple) page.readTuples().get(i)).getAttributes().contains(key)) {
				return true;
			}
		}
		return false;

	}

	public void insertTuple(Hashtable<String, Object> htblColNameValue,String strTableName) {
		int pageNo = 0;
		Page currentPage = null;
		if (noRows == maxRows) {
			pageNo = pages.size();
			//System.out.println(pageNo);
			currentPage = new Page();
			pages.add(pageNo);
			noRows = 0;
		} else {
			pageNo = pages.size() - 1;
			currentPage = readPage(pageNo,strTableName);
		}
		Vector<Object> attrs = new Vector<Object>(attrNo);
		Set<String> names = htblColNameValue.keySet();
		int key = 0;
		for (String name : names) {
			Object value = htblColNameValue.get(name);
			// System.out.println(name +value);
			if (checkType(name, value)) {
				attrs.add((String) value);
				if (name.equals(tableKey)) {
					key = attrs.size();
				}
			} else {
				System.out.println("Invalid Input for" + name + " " + value);
				return;
			}

		}
//		tupleToInsert.setLocInPage(currentPage.readTuples().size());
		currentPage.addTuple(new Tuple(attrs, key, null,currentPage.readTuples().size()));

		writePage(currentPage, pageNo,strTableName);
		readPage(pageNo,strTableName);
		noRows++;

	}

	public boolean checkType(String name, Object value) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File("./src/main/resources/metadata.csv")));
			reader.readLine();
			String line = "";
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
//				System.out.println(parts[1]);
//				System.out.println(parts[2]);

				if ((name.equals(parts[1])) && (value.getClass().getName().equals(parts[2]))) {
					
					return true;
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;

	}
	//Serializing A page and saving it to a .ser file 
	public void writePage(Page page, int indicator,String strTableName) {

		try {
			FileOutputStream fileOut = new FileOutputStream("./src/main/resources/data/" + strTableName + "P" + indicator + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
//			System.out.println("Serialized data is saved in " + strTableName + " P" + indicator + ".ser");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//Deserializing a page to read it
	public Page readPage(int indicator, String strTableName) {

		try {
			FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + strTableName + "P" + indicator + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page e = (Page) in.readObject();
			e.readTuples().forEach((b) -> {
//				System.out.print("TUPLE :");
//				System.out.println(((Tuple) b).getAttributes());

			});

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

	public void deleteTuple(Hashtable<String, Object> htblColNameValue , String strTableName, String clusteringKey) {
		Page currentPage = null;
		Vector<Object> attrs = new Vector<Object>(attrNo);

		Vector<String> colNames = new Vector<String>();
		Set<String> names = htblColNameValue.keySet();
		
//		System.out.println(names);
//		System.out.println(htblColNameValue);
		int key = -1;
		for (String name : names) {

			Object value = htblColNameValue.get(name);
			// System.out.println(name +value);
			if (checkType(name, value)) {
				if(value.getClass().getName().equals("java.lang.Double")||value.getClass().getName().equals("java.lang.Integer")) {
					attrs.add(String.valueOf(value));
				}else if (value.getClass().getName().equals("java.util.Date")){
					Date date = (Date) value;
					DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
					String strDate = dateFormat.format(date);
					attrs.add(strDate);
				}
				else {	attrs.add((String) value);}
			
				colNames.add(name);
				if (name.equals(tableKey)) {
					key = attrs.size() - 1;
				}
			} else {
				System.out.println("Invalid Input for" + name + " " + value);

				return;
			}

		}

		Tuple tupleToDelete = new Tuple(attrs, key, colNames,0);
		for (int i = 0; i < pages.size(); i++) {
			currentPage = readPage(i,strTableName);
			Vector<Tuple> tempVector = currentPage.readTuples();
			for (int j = 0; j < tempVector.size(); j++) {
				if (tempVector.get(j).compareTo(tupleToDelete) == 0) {
					tempVector.remove(j);
					j--;
					if (tempVector.size() == 0 && i != pages.size() - 1) {
						shiftPagesUp(i,strTableName);
					} else if (!(tempVector.size() == 0) && i != pages.size() - 1) {
						writePage(currentPage, i,strTableName);
					} else if (tempVector.size() == 0 && i == pages.size() - 1) {
						removePage(i);
					}
				}
			}
		}
	}

	public void removePage(int pageNo) {
		pages.remove(pageNo);
		File toBeDeleted = new File(tableName + "P" + pageNo + ".ser");

		if (toBeDeleted.delete()) {
			System.out.println("File" + pageNo + "Deleted");
		}
	}

	public void shiftPagesUp(int startPage,String strTableName) {
		pages.remove(startPage);
		for (int i = startPage; i < pages.size(); i++) {
			pages.set(i, i);
			Page currentPage = this.readPage(i + 1,strTableName);
			this.writePage(currentPage, i,strTableName);
		}
		File toBeDeleted = new File(tableName + "P" + pages.size() + ".ser");
		if (toBeDeleted.delete()) {
			System.out.println("File" + pages.size() + "Deleted");
		}
	}

	public int insertSortedTuple(Hashtable<String, Object> htblColNameValue,String strTableName,String clusteringKey) throws DBAppException {
		int ret;
//		System.out.println(this.pages);
		Table table = this;
		Vector<Object> attrs = new Vector<Object>();

		Vector<String> colNames = new Vector<String>();
		Set<String> names = htblColNameValue.keySet();

		int key = -1;
		for (String name : names) {

			Object value = htblColNameValue.get(name);
			// System.out.println(name +value);
			if (checkType(name, value)) {
				if(value.getClass().getName().equals("java.lang.Double")||value.getClass().getName().equals("java.lang.Integer")){
					attrs.add(String.valueOf(value));
				}else if (value.getClass().getName().equals("java.util.Date")){
					Date date = (Date) value;
					DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
					String strDate = dateFormat.format(date);
					attrs.add(strDate);
				}else {attrs.add((String) value);}
				
				colNames.add(name);
				if (name.equals(clusteringKey)) {
					key = attrs.size() - 1;
				}
			} else {
				System.out.println("Invalid Input for" + name + " " + value);

				return -1;
			}

		}

		Tuple tupleToInsert = new Tuple(attrs, key, colNames,-1);
		
		Page currentPage ;
		for (int i = 0; i < pages.size() ; i++) {
			currentPage = readPage(i,strTableName);
			Vector<Tuple> tempVector = currentPage.readTuples();
			if(tempVector.size()>=maxRows) {
				i++;
				
			}

		}
//		System.out.println(pages.size());

		currentPage = readPage(pages.size() - 1,strTableName);
//		System.out.println(pages.size());
		Vector<Tuple> tempVector = currentPage.readTuples();
		if (tempVector.size() == maxRows) {
//			System.out.println("Da el overflow el ghaby");
//			this.incPage();
			currentPage = new Page();
			this.pages.add(this.pages.size());
//			System.out.println(pages);
//			System.out.println(pages.size());
			tupleToInsert.setLocInPage(0);
			currentPage.addTuple(tupleToInsert);
			tempVector=currentPage.readTuples();
//			System.out.println(tempVector);
			writePage(currentPage, pages.size() - 1,strTableName);
			Vector<Table> tables = readTables();
//			for(Table table:tables) {
//				if(table.getName().equals(strTableName)) {
//					
//				}
//			}
			for(int i=0;i<tables.size();i++) {
				if(tables.get(i).getName().equals(strTableName)) {
					tables.remove(i);
					tables.add(this);
				}
			}
			writeTables(tables);
			

		} else {
			if (currentPage.readTuples().size() > 0) {
				tupleToInsert.setLocInPage(currentPage.readTuples().size());
				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				writePage(currentPage, pages.size() - 1,strTableName);

			} else {
				tupleToInsert.setLocInPage(currentPage.readTuples().size());
				currentPage.addTuple(tupleToInsert);
				currentPage.sort();

				writePage(currentPage, pages.size() - 1,strTableName);
			}
		}
		return pages.size() - 1;
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
			e.printStackTrace();
		}
	}
	private Vector<Table> readTables() {

		try {
			FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + "tablesArray" + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Vector<Table> e = (Vector<Table>) in.readObject();
//			System.out.println(e);
		

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

	public Vector<Integer> getPages() {
		return pages;
	}

	public void shiftingPages(Tuple overFlowTuple, int index, String strTableName) {
//		System.out.println("Dakhalt Shifting" + pages);
		if (index >= pages.size()) {
			int pageNo = pages.size();
			Page currentPage = new Page();
			pages.add(pageNo);
//			System.out.println(this.pages);
			overFlowTuple.setLocInPage(currentPage.readTuples().size());
			currentPage.addTuple(overFlowTuple);
			writePage(currentPage, index,strTableName);
		} else {
			Page currentPage = readPage(index,strTableName);
			if (currentPage.readTuples().size() < maxRows) {
				overFlowTuple.setLocInPage(currentPage.readTuples().size());
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				writePage(currentPage, index,strTableName);
			} else {
				overFlowTuple.setLocInPage(currentPage.readTuples().size());
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				Tuple newOverFlow = (Tuple) currentPage.readTuples().remove(maxRows);
				writePage(currentPage, index,strTableName);
				shiftingPages(newOverFlow, ++index,strTableName);
			}

		}

	}

	public static void main(String[] args) throws DBAppException {
	
	}

	public void setPages(Vector<Integer> pages) {
		this.pages = pages;
	}

	public String getTableKey() {
		return tableKey;
	}

	public void setTableKey(String tableKey) {
		this.tableKey = tableKey;
	}

}
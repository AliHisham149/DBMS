//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.util.ArrayList;
//import java.util.Hashtable;
//import java.util.Vector;
//
public class SQLTerm {
//	
//	
	  String _strTableName;  String _strColumnName;  String
	  _strOperator; Object _objValue;
//	 
//	
//	public Table opertaion (String _strTableName,String _strColumnName,String _strOperator,Object _objValue) {
//	String op;
//	
//	Vector <Table> tables = readTables();
//	
//	Table out;
//		tables.forEach((c) -> {
//			if (c.getName().equals(_strTableName)) {
//				Table main = c;	
//			}
//	});
//		
//		Vector <String> columnNames = (Vector<String>) main.getHtblColNameType().keySet();
//		int j;
//		
//		for (int i =0 ; i < columnNames.size() ; i++ ) {
//			if(columnNames.get(i)==_strColumnName) {
//				j=i;
//			}
//		}
//		Vector<Object> attrs = new Vector<Object>(columnNames.size());
//		
//		Vector <Integer> pages = main.getPages();
//		
//		Page current;
//		Vector <Tuple> tuples;
//		Vector <Object> temp;
//		Hashtable  <String, Object> hash = new Hashtable<String,Object>() ;
//		for (int i=0 ; i<pages.size();i++)
//		{
//			current = main.readPage(i,main.getName());
//			tuples = current.readTuples();
//			for (int k=0 ; k<tuples.size(); k++) {
//				temp = tuples.get(k).getAttributes();
//				if(checkop(_strOperator,(String)_objValue,(String)temp.get(j))) {
//					
//					for (int m=0 ;m<temp.size();m++) {
//						hash.put(columnNames.get(m),temp.get(m));
//						
//					}
//					
//					try {
//						out.insertSortedTuple(hash,main.getName(),main.getTableKey());
//					} catch (DBAppException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					hash =null;
//				}
//			}
//		}
//		
//		return out;
//		
//}
//	private Vector<Table> readTables() {
//		try {
//			FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + "tablesArray" + ".class");
//			ObjectInputStream in = new ObjectInputStream(fileIn);
//			ArrayList<Table> e = (ArrayList<Table>) in.readObject();
//			System.out.println(e);
//		
//
//			in.close();
//			fileIn.close();
//			return e;
//		} catch (IOException i) {
//			i.printStackTrace();
//			return null;
//
//		} catch (ClassNotFoundException c) {
//			System.out.println("Page class not found");
//			c.printStackTrace();
//			return null;
//		}
//	}
//	public boolean checkop(String _strOperator ,String value , String real) {
//		
//		
//		if(_strOperator.equals("=") ){
//			if(value.equals(real))
//				return  true;
//			
//		}
//			
//		else
//			if(_strOperator.equals(">")) {
//				if(real.compareTo(value) >0)
//					return true;
//				
//			}
//			else 
//				if (_strOperator.equals("<")) {
//					if(real.compareTo(value)<0)
//						return true;
//					
//				}
//					
//				else	
//					if(_strOperator.equals("!="))	
//						if(!real.equals(value))	
//							return true;
//					else	
//						if(_strOperator.equals(">="))
//							if(real.compareTo(value) >0 || real.equals(value))
//								return true;
//						else	
//							if (_strOperator.equals("<="))
//								if(real.compareTo(value) <0 || real.equals(value))
//									return true;
//		
//		
//		
//		
//		return false;
//	}
//
//	
}

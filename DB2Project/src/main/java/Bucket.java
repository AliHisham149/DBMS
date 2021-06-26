import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable  {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Vector <Object> location= new Vector <Object>();
	
	
	public Bucket() {
		
	}
	public void insert( int p , int key) {
		
			this.addlocation(p, key);;
			
			
		}
		
	
public void addlocation( int page , int key) {
		
		Vector <Integer> temp = new Vector <Integer>();
		temp.add(page);
		temp.add(key);
		if(location.indexOf(temp)==-1)
		location.add(temp);
//		System.out.println(location);
		
	}
	public void deleteLocation( int key) {
		for(int i=0;i<location.size();i++) {
			Vector <Integer> temp = (Vector<Integer>) location.get(i);
			if(temp.get(1)==key){
				location.remove(i);
			}
		}
	}
	public Vector<Object> readlocation(){
		return this.location;
	}
	public void writeLocation(Vector<Object> x){
		this.location = x;
	}
		public void deleteTuple(Tuple t) {
			int x=t.getKeyIndex();
			this.deleteLocation(x);
		}
		
//		public Page readPage(int indicator, String strTableName) {
//
//			try {
//				FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + strTableName + "g" + indicator+ ".ser");
//				ObjectInputStream in = new ObjectInputStream(fileIn);
//				Page e = (Page) in.readObject();
//				e.readTuples().forEach((b) -> {
////					System.out.print("TUPLE :");
////					System.out.println(((Tuple) b).getAttributes());
//
//				});
//
//				in.close();
//				fileIn.close();
//				return e;
//			} catch (IOException i) {
//				i.printStackTrace();
//				return null;
//
//			} catch (ClassNotFoundException c) {
//				System.out.println("Page class not found");
//				c.printStackTrace();
//				return null;
//			}
//		}
//		
//		public void writePage(Page page, int indicator,String strTableName) {
//
//			try {
//				FileOutputStream fileOut = new FileOutputStream("./src/main/resources/data/" + strTableName + "g" + indicator + ".ser");
//				ObjectOutputStream out = new ObjectOutputStream(fileOut);
//				out.writeObject(page);
//				out.close();
//				fileOut.close();
////				System.out.println("Serialized data is saved in " + strTableName + " P" + indicator + ".ser");
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	
	

}


import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Vector;

public class Page implements Serializable {

	private static final long serialVersionUID = 1214738099184642172L;
	private Vector<Tuple> tuples = new Vector<Tuple>();
	private Object max;
	private Object min;
	
		

	public void addTuple(Tuple tuple) {
		tuples.add(tuple);
	}
	public Vector<Tuple> readTuples() {
		return tuples;
	}
	public long getID() {
		return serialVersionUID;
	}
	public void sort(){
		Collections.sort(tuples);
	}
	public static void main(String[] args) {
		
	}
	
	public static int binarySearchPage(Table t,Comparable key) throws IOException, ClassNotFoundException {
		int low=0;
		Vector<Integer> pages=t.getPages();
		int high=pages.size()-1;
		int mid;
		while(low<=high) {
			mid=low +(high-low)/2;
			FileInputStream fileIn= new FileInputStream("./src/main/resources/data/" + t.getName() + "P" + key + ".class");
			ObjectInputStream in= new ObjectInputStream(fileIn);
			Page p1=(Page) in.readObject();
			in.close();
			fileIn.close();
			Comparable max= (Comparable) p1.max;
			// Check if key is present at mid (last record in page)
			if (max.compareTo(key)==0 ) {
				return mid ;
			}
			// If key greater than max, ignore left half
			if (max.compareTo(key) < 0)
				low  = mid + 1;
			// If key is smaller than max, ignore right half
			else
				high = mid - 1;
		}
		// if we reach here, then element was not present
		return -1 ;
	} 

}
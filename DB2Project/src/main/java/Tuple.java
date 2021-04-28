

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Tuple implements Serializable, Comparable<Object> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int keyIndex;
	private Vector<Object> attrs = new Vector<Object>();
	private Vector<String> colName=new Vector<String>();
	
	public Tuple(Vector<Object> attrs,int keyIndex,Vector<String> colName) {
		this.attrs=attrs;
		this.keyIndex=keyIndex;
		this.colName=colName;
		
	}
	public Vector<Object> getAttributes() {
		return attrs;
	}
	public void setAttributes(Vector<Object> attrs2) {
		this.attrs=attrs2;
	}
	@Override
	public int compareTo(Object tuple1) {
		boolean flag=true;
		Tuple min=(((Tuple)tuple1).attrs.size()>=this.attrs.size())?this:(Tuple)tuple1;
		Tuple max=(((Tuple)tuple1).attrs.size()<this.attrs.size())?this:(Tuple)tuple1;
		for(int i=0;i<min.attrs.size();i++){
		for(int j=0;j<max.attrs.size();j++){
		if(min.colName.get(i).equals(max.colName.get(j))){
			if(!min.attrs.get(i).equals(max.attrs.get(j))){
				flag=false;
}
		}	
		}
		}
		if(flag){
		return 0;}
		try{
			Double thisAttr=Double.parseDouble(""+this.attrs.get(keyIndex));
			Double otherAttr=Double.parseDouble((""+((Tuple)tuple1).attrs.get(((Tuple) tuple1).getKeyIndex())));
			if(thisAttr>otherAttr){return 1;}
			else if(thisAttr<otherAttr){return -1;}
			else {return 2;}
		}catch (NumberFormatException e){
			String thisAttr=(""+this.attrs.get(keyIndex));
			String otherAttr=(""+((Tuple)tuple1).attrs.get(((Tuple)tuple1).getKeyIndex()));
			if(thisAttr.compareTo(otherAttr)>0){return 1;}
			else if((thisAttr).compareTo(otherAttr)<0){return -1;}
			else return 2;
		}
		catch(ArrayIndexOutOfBoundsException e){
			return 1;
		}
	}
	public int getKeyIndex() {
		return keyIndex;
	}
	public String toString(){
		String e="";
		for(int i=0;i<attrs.size();i++){
			e=e+" "+attrs.get(i);
		}
		return e;
		
	}
	public static void main (String[] args){
		Vector a1=new Vector();
		a1.add(2);
		a1.add("Ahmed Noor");
		a1.add("2.5");
		Vector b1=new Vector();
		b1.add(2);
		b1.add("Ahmed Noor");
		b1.add("2.5");
		Vector a2=new Vector();
		a2.add("id");
		a2.add("name");
		a2.add("gpa");
		Vector b2=new Vector();
		b2.add("id");
		b2.add("name");
		b2.add("gpa");
		Tuple t1=new Tuple(a1,0,a2);
		Tuple t2=new Tuple(b1,0,b2);
		System.out.println(t2.compareTo(t1));
	}
}
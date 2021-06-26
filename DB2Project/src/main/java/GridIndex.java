import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class GridIndex implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1214738099184642172L;
	
	private Vector<String> colnames ;
	private String tablename;
	private Vector<Object> rangecols;
	private Vector<Integer> buckets;
	private Vector<String> typecols;
	private boolean writtenBuckets=false;
	public GridIndex (Table table,Vector<String> colnames) {
//		System.out.println("Sherlocked");
		  this.tablename = table.getName();
		  this.colnames = colnames;
		  
		  int size = colnames.size(); 
		  int bucketSize=1;
		  this.typecols = new Vector<String> (colnames.size()); 
		  for(int i =0;i<colnames.size();i++) {
			  typecols.add(i, table.getHtblColNameType().get(colnames.get(i)));
		  }
		  for(int i=0;i<size;i++) {
			  bucketSize = 10*bucketSize;
		  }
		  rangecols=new Vector<Object>(); 
		  this.buckets=new Vector<Integer>(bucketSize);
//		  for(int i=0;i<bucketSize;i++) {
//			 this.buckets.add(-1);
//		  }
		  
		  String min;
		  String max; 
		  String type; 
		  
		  for ( int i =0 ; i<size ; i++) {
			  
		  min=table.getcolmin(colnames.get(i));
//		  System.out.println(min);
		  max= table.getcolmax(colnames.get(i));
//		  System.out.println(max);
		  type=table.getcoltype(colnames.get(i));
//		  System.out.println(type);
		  getrange(min,max,type,i);
		 
		 }
		  

	}
	
	// checks if a bucket exists for certain index
	public boolean bucketexists(int n) {
		for(int i=0;i<buckets.size();i++) {
//			System.out.println(buckets[i]);
			if(buckets.get(i)==n) {
				return true;
			}
		}
		return false;
	}
	public void deleteGrid (Tuple t , int p) {
		Vector<String> tuplecols = t.getColnames();
		Vector <Object> tupleattrs =t.getAttributes();
		int key = t.getLocInPage();
		int [] index= new int [colnames.size()];
		for(int i = 0;i<this.colnames.size();i++) {
			for (int j =0;j<tuplecols.size();j++) {
				if(tuplecols.get(j).equals(colnames.get(i)))
				{
					System.out.println();
					index[i]= getplace(tupleattrs.get(j),i);
				}
			}
			
		}
		int temp = this.getindex(index);
		Bucket b = this.readBucket(temp, tablename);
		b.deleteTuple(t);
		
	}
	public void updateGrid(Tuple t ,int p , Tuple newt) {
		Vector<String> tuplecols = t.getColnames();
		Vector <Object> tupleattrs =t.getAttributes();
		int key = t.getLocInPage();
		int [] index= new int [colnames.size()];
		for(int i = 0;i<this.colnames.size();i++) {
			for (int j =0;j<tuplecols.size();j++) {
				if(tuplecols.get(j).equals(colnames.get(i)))
				{
					index[i]= getplace(tupleattrs.get(j),i);
				}
			}
			
		}

		int temp = this.getindex(index);
//		System.out.println(temp);
		buckets = this.getBuckets();
		if(temp==0) {
			return;
		}
		if(this.bucketexists(temp)) {
			Bucket b= readBucket(temp,this.tablename);
			b.insert(p, key);
			this.writeBucket(b, temp, tablename);
		}
		else
		{
			Bucket b = new Bucket();
			b.insert(p,key);
			if(buckets.indexOf(temp)==-1)
			buckets.add(temp);

			this.writeBucket(b, temp, tablename);
		}
		
	}
	
	
	public int getindex(int[] index) {
		int temp=0;
		for(int i=0;i<index.length-1;i++) {
			if(i!=-1){
			
				if(i==0) {
					if(index[i]==-1 || index[i+1]==-1) {
						return -1;
					}
					temp=(index[i]*10)+index[i+1];
//					System.out.println("Awel Index"+index[i]);
//					System.out.println("Tany Index"+index[i+1]);
					i++;
				}
				else 
				{
//					System.out.println(temp);
					temp= temp+ temp*10+index[i+1];
//					System.out.println("Awel Index2"+index[i]);
//					System.out.println("Tany Index2"+index[i+1]);
				}
			}
			
			
		}
		return temp;
	}

	public Vector<Integer> insertintogrid(Tuple t, int p) {
		Vector<String> tuplecols = t.getColnames();
		Vector <Object> tupleattrs =t.getAttributes();
		int key = t.getLocInPage();
		int [] index= new int [colnames.size()];
		
		//getting the place to insert in the form of an array
		//index contains the index of each col to insert in
		//-1 means doesn't exit ??
		for(int i = 0;i<this.colnames.size();i++) {
			for (int j =0;j<tuplecols.size();j++) {
				if(tuplecols.get(j).equals(colnames.get(i)))
				{
//					System.out.println(colnames.get(i) + " Yarab " + this.getname() + " "+ this.getColnames() + i);
					index[i]= getplace(tupleattrs.get(j),i);
				}
			}
			
		}
//		System.out.println("Before"+this.buckets);
		int temp = this.getindex(index);
		if(temp<0) {
			return this.buckets;
		}
//		if(writtenBuckets)
//		this.buckets = readBuckets(this.tablename);
//		System.out.println(buckets);
//		System.out.println(temp);
		if(bucketexists(temp)) {
//			System.out.println("Existing Bucket");
			Bucket b= readBucket(temp,this.tablename);
			b.insert(p,key);
			this.writeBucket(b, temp, tablename);
			writtenBuckets=true;
			
		}
		else
		{
//			System.out.println("Temp Abl"+buckets[temp]);
			Bucket b = new Bucket();
			b.insert( p,key);
			writtenBuckets=true;
//			System.out.println("Temp value" + temp);
			if(temp<0) {
				return this.buckets;
			}
			if(buckets.indexOf(temp)==-1)
			this.buckets.add(temp);
//			System.out.println("Temp Ba3d"+buckets[temp]);
			writtenBuckets=true;
			this.writeBucket(b, temp, tablename);
//			System.out.println("Ba3d ma dakhalt"+this.buckets);
			this.setBuckets(this.buckets);
		}
		
		return this.buckets;
		

		

		
		
	}
	


	public int getplace(Object object, int n) {
		Vector<Object > habiba= (Vector <Object>)this.rangecols.get(n);
		
		// temp da gowah 10 arrays btoo3 el range
		Vector<Object > temp = new Vector<Object>();
		for(int i=0;i<habiba.size();i++) {
			temp.add(habiba.get(i));
		}
//		System.out.println(this.rangecols.size());
		
		//type of col
		String type = this.typecols.get(n);
//		System.out.println(habiba);
		
		if(type.equals("java.lang.Integer")) {
			int tocompare = Integer.parseInt(object.toString());
			
			//looping through el ranges
			
			for (int i =0;i< temp.size();i++) {
				Vector<Object> temp2 = (Vector<Object>) temp.get(i);
				int min = Integer.parseInt( temp2.get(0).toString());
				int max = Integer.parseInt( temp2.get(1).toString());
				
				
				if(tocompare >= min && tocompare <= max) {
					return i;
				}
			}
			
		}
		
		if(type.equals("java.lang.Double")) {
//				System.out.println("El object abl "+object);
			double tocompare = Double.parseDouble(object.toString());
//			System.out.println("El object ba3d "+ tocompare);
			for(int i =0 ;i<temp.size();i++) {
			Vector <Object> temp2 = (Vector<Object>) temp.get(i);
			double min = Double.parseDouble(temp2.get(0).toString());
			double max = Double.parseDouble(temp2.get(1).toString());
//			System.out.println("Da max "+ max);
//			System.out.println("Da value "+tocompare);
//			System.out.println("Da min "+min);
			if(tocompare >= min && tocompare <= max) {
//				System.out.println("Da max "+ max);
//				System.out.println("Da value "+tocompare);
//				System.out.println("Da min "+min);
				
//				System.out.println("Da keda dakhaal");
//				System.out.println("");
				return i;
			}
			}
		}
		if(type.equals("java.lang.String")) {
			
				String string = object.toString();
				int comp=0;
				for(int i=0;i<string.length();i++) {
					comp+= (int)string.charAt(i);
				}
				
				for(int i=0;i<temp.size();i++) {
					Vector<Object> temp2 = (Vector<Object>) temp.get(i);
					int min = Integer.parseInt( temp2.get(0).toString());
					int max = Integer.parseInt( temp2.get(1).toString());
					
					
					if(comp >= min && comp <= max) {
						return i;
					}
					
				}
				
			
		}
		
		if(type.equals("java.util.Date ")) {
			String tocompare = object.toString();
			int daycomp = Integer.parseInt(tocompare.substring(8));
			int monthcomp = Integer.parseInt(tocompare.substring(5, 7));
			int yearcomp =Integer.parseInt(tocompare.substring(0,4));
			
			for(int i =0 ;i<temp.size();i++) {
				Vector <Object> temp2 = (Vector<Object>) temp.get(i);
				String datemin = temp2.get(0).toString();
				String datemax = temp2.get(1).toString();
				
				int daymin = Integer.parseInt(datemin.substring(8));
				int monthmin = Integer.parseInt(datemin.substring(5, 7)) ;
				int yearmin =Integer.parseInt(datemin.substring(0,4));
				
				int daymax = Integer.parseInt(datemax.substring(8));
				int monthmax = Integer.parseInt(datemax.substring(5, 7));
				int yearmax =Integer.parseInt(datemax.substring(0,4));
				
				if(yearcomp <= yearmax && yearcomp >= yearmin) {
					if(monthcomp <= monthmax && monthcomp >= monthmin) {
						if(daycomp <= daymax && daycomp >= daymin) {
							//System.out.println("gowa date");
							return i;
							
						}
					}
				}
			}
		}
		
		
		return 0;
	}

	
	// missing date type

	public Vector<String> getColnames() {
		return colnames;
	}

	public void setColnames(Vector<String> colnames) {
		this.colnames = colnames;
	}

	public Vector<Integer> getBuckets() {
		return buckets;
	}

	public void setBuckets(Vector<Integer> buckets) {
		this.buckets = buckets;
	}

	public void getrange(String min, String max, String type, int y) {
		int r = 0;
		double n;
		int m;
		Vector<Object> toenter = new Vector<Object>(10);
//		System.out.println(type);
//		Hashtable<String,Vector<String[]>> noiceRanges = new Hashtable<String,Vector<String[]>>();
//		toenter.add(0, type);
		// 7) +3 A column can have any of the 6 types
		if (type.equals("java.lang.Integer")) {
			//System.out.println("ana hena");
			r = (Integer.parseInt(max) - Integer.parseInt(min)) / 10;
			int minimum = Integer.parseInt(min);
			for (int i = 0; i < 10; i++) {
				Vector<Integer> temp = new Vector<Integer>(2);
//				System.out.println(temp);
				temp.add(0,minimum);
				temp.add(1,minimum+r);
				minimum += (r);
				toenter.add(i, temp);
			}

		}
		if (type.equals("java.lang.Double")) {
//			System.out.println(max);
//			System.out.println(min);
			n = (Double.parseDouble(max) - Double.parseDouble(min)) / 10;
//			System.out.print(n);
//			Double rem = (Double.parseDouble(max) - Double.parseDouble(min)) % 1;
			double minimum = Double.parseDouble(min);
			for (int i = 0; i < 10; i++) {
				Vector<Object> temp = new Vector<Object>(2);
//				System.out.println(temp);
				temp.add(0,minimum);
				temp.add(1,minimum+n);
				minimum += (n);
				toenter.add(i, temp);
//				System.out.println(toenter);
			}

		}

		if (type.equals("java.lang.String")) {
			int minasci=0;
			int maxasci=0;
			for (int i = 0; i <min.length(); i++) {
			char c1 = min.charAt(i);
			minasci=minasci+(int)c1;
			
			}
			for (int i = 0; i <max.length(); i++) {
				char c1 = max.charAt(i);
				maxasci=maxasci+(int)c1;
				
				}
			r=(maxasci-minasci)/10;
			for (int i = 0; i <10; i++) {
				
				Vector<Integer> temp = new Vector<Integer>(2);
//				System.out.println(temp);
				temp.add(0,minasci);
				temp.add(1,minasci+r);
				minasci += (r );
				toenter.add(i, temp);
				}
				
			
				
			
					
		
		}
		if(type.equals("java.util.Date")) {
			
		
//			System.out.println("dakhal date");
			
			int yearmin = Integer.parseInt(min.substring(0, 4));
			int monthmin = Integer.parseInt(min.substring(5, 7));
			int daymin= Integer.parseInt(min.substring(9, 10));
//			System.out.println(yearmin);
//			System.out.println(monthmin);
//			System.out.println(daymin);
			int yearmax= Integer.parseInt(max.substring(0, 4));
			int monthmax = Integer.parseInt(max.substring(5, 7));
			int daymax = Integer.parseInt(max.substring(9, 10));
			
			int rangemonth =(monthmax-monthmin)/10;
			int rangeyear=(yearmax+1-yearmin)/10;
			int rangeday = (daymax-daymin)/10;
//			System.out.println(rangemonth);
			//System.out.println(monthmin);
			for(int i =0;i<10;i++) {
				
				Vector<Object> temp =new Vector<Object>(2);
				if(daymin>30) {
					daymin=daymin-30;
					monthmin++;
				}
				if(monthmin>12) {
					monthmin=monthmin-11;
					yearmin++;
				}
				String monthre ="0";
				String datetemp="";
				if(monthmin<12) {
					if(daymin<10)
						datetemp=yearmin+"-"+monthre+monthmin+"-"+monthre+daymin;
					else
						datetemp=yearmin+"-"+monthre+monthmin+"-"+daymin;
					
				}
				else {
					if(daymin<10)
						datetemp=yearmin+"-"+monthmin+"-"+monthre+daymin;
					else
						datetemp=yearmin+"-"+monthmin+"-"+daymin;
					 
				}
				//System.out.println(datetemp);
				temp.add(0,datetemp);
				
				daymin+= rangeday;
				if(daymin>30) {
					daymin=daymin-30;
					monthmin++;
				}
				monthmin+=rangemonth;
				if(monthmin>12) {
					monthmin=monthmin-11;
					yearmin++;
				}
				yearmin+=rangeyear;
				
				if(monthmin<12) {
					if(daymin<10)
						datetemp=yearmin+"-"+monthre+monthmin+"-"+monthre+daymin;
					else
						datetemp=yearmin+"-"+monthre+monthmin+"-"+daymin;
					
				}
				else {
					if(daymin<10)
						datetemp=yearmin+"-"+monthmin+"-"+monthre+daymin;
					else
						datetemp=yearmin+"-"+monthmin+"-"+daymin;
					
				}
				//System.out.println(datetemp);
				temp.add(1,datetemp);
				//System.out.println("test"+i);
				toenter.add(i, temp);
				daymin++;
				
				
			
			}
				
			
			
		}
//		System.out.println(toenter);
//		for(int j=0;j<toenter.size();j++) {
//			System.out.println(toenter.get(j));
//		}
		this.setRangecols(toenter, y);
//		System.out.println(toenter.toString());
	}
	public String getname() {
		return tablename;
	}
	public void writeBucket(Bucket bucket, int indicator, String strTableName) {
		String temp = "";
		for(int i=0; i<this.colnames.size();i++) {
			temp = temp + colnames.get(i);
		}
		try {
			FileOutputStream fileOut = new FileOutputStream(
					"./src/main/resources/data/" + strTableName + "gridbuckets" + indicator +temp+ ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(bucket);
			out.close();
			fileOut.close();
//			System.out.println("Serialized data is saved in " + strTableName + " P" + indicator + ".ser");
		} catch (IOException e) {
//			e.printStackTrace();
		}
	}
	public Bucket readBucket(int indicator, String strTableName) {
		String temp = "";
		for(int i=0; i<this.colnames.size();i++) {
			temp = temp + colnames.get(i);
		}
		try {
			FileInputStream fileIn = new FileInputStream(
					"./src/main/resources/data/" + strTableName + "gridbuckets" + indicator +temp+ ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Bucket e = (Bucket) in.readObject();

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
	public Vector<Object> getRangecols() {
		return rangecols;
	}
	public void setRangecols(Vector<Object> rangecols, int i) {
		this.rangecols.add(i, rangecols);
	}
	public static char getCharByID(int n) {
		char output = 'x';
		switch(n) {
		case 1: output ='a';break;
		case 2: output ='b';break;
		case 3: output = 'c';break;
		case 4: output = 'd';break;
		case 5: output = 'e';break;
		case 6: output = 'f';break;
		case 7: output = 'g';break;
		case 8: output = 'h';break;
		case 9: output = 'i';break;
		case 10: output = 'j';break;
		case 11: output = 'k';break;
		case 12: output = 'l';break;
		case 13: output ='m';break;
		case 14: output = 'n';break;
		case 15: output = 'o';break;
		case 16: output = 'p';break;
		case 17: output = 'q';break;
		case 18: output = 'r';break;
		case 19: output = 's';break;
		case 20: output = 't';break;
		case 21: output = 'u';break;
		case 22: output ='v';break;
		case 23: output = 'w';break;
		case 24: output = 'x';break;
		case 25: output = 'y';break;
		case 26: output = 'z';break;
		}
		return output;
		
	}
	public static int checkIDinAlphabet(char y) {
		int x =0;
		switch(y) {
		case 'a':x=1;break;
		case 'b':x=2;break;
		case 'c':x=3;break;
		case 'd':x=4;break;
		case 'e':x=5;break;
		case 'f':x=6;break;
		case 'g':x=7;break;
		case 'h':x=8;break;
		case 'i':x=9;break;
		case 'j':x=10;break;
		case 'k':x=11;break;
		case 'l':x=12;break;
		case 'm':x=13;break;
		case 'n':x=14;break;
		case 'o':x=15;break;
		case 'p':x=16;break;
		case 'q':x=17;break;
		case 'r':x=18;break;
		case 's':x=19;break;
		case 't':x=20;break;
		case 'u':x=21;break;
		case 'v':x=22;break;
		case 'w':x=23;break;
		case 'x':x=24;break;
		case 'y':x=25;break;
		case 'z':x=26;break;
		
		
		}
		return x ;
		
	}
	public static void main(Vector<String> args) {

		
		/*
		 * GridIndex grid = new GridIndex(); Object [] array = new Object[3]; Vector<String>
		 * temp = {"1","100","java.lang.Integer","0"}; array[0]= temp; Vector<String> temp2 =
		 * {"0.0","10.0","java.lang.double","1"}; array[1]= temp2; Vector<String> temp3 =
		 * {"a","z","java.lang.String","2"}; array[2]=temp3;
		 * 
		 * for (int i=0;i<3;i++) { Vector<String> te = (Vector<String>) array[i];
		 * grid.getrange(te[0], te[1], te[2], i); } Object [] toprint =
		 * grid.getRangecols(); for(int i =0 ; i<toprint.size();i++) { Object [] fuck =
		 * (Vector<Object>) toprint[i]; for(int j =0 ;j<fuck.size();j++) { Object [] fuck2 =
		 * (Vector<Object>) fuck[j]; for(int k =0 ;k<2;k++) {
		 * System.out.print(fuck2[k].toString() + ", "); } } System.out.println(""); }
		 */
		/*
		 * Vector<String> t= {"100","200","java.lang.Integer","0"}; Object type
		 * ="java.lang.Integer"; Vector<Object> toenter=new Object [10]; int r =
		 * (Integer.parseInt(t[1]) - Integer.parseInt(t[0])) / 10; int minimum =
		 * Integer.parseInt(t[0]); for (int i = 0; i < 10; i++) { int [] temp = new
		 * int[2]; temp[0] = minimum; temp[1] = (minimum + r); minimum += (r + 1);
		 * toenter[i] = temp; //System.out.println(temp[0] + " " + temp[1]); } int x =
		 * 110; if(type.equals("java.lang.Integer")) { for(int i=0;i<toenter.size();i++)
		 * { int [] temo = (int []) toenter[i]; int y= temo[0]; int z = temo[1];
		 * //if(x>=y && x <=z) // System.out.println(i); }
		 * 
		 * } int l=0; int [][][] fuck = new int [5][5][5]; for(int i =0 ; i<5;i++) {
		 * for(int j=0;j<5;j++) { for(int k=0;k<5;k++) { fuck[i][j][k]=l; l++; } } }
		 * for(int i =0 ; i<5;i++) { for(int j=0;j<5;j++) { for(int k=0;k<5;k++) {
		 * System.out.print(fuck[i][j][k] +" , "); } } System.out.println(""); }
		 */
	}

}

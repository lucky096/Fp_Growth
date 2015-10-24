import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;

public class Vertical_fpgrowth 
{
	static int total_fp=0;
	public static void main(String[] args) throws Exception 
	{
		long startTime;
   	  	//time started
   	  	startTime = System.currentTimeMillis();
    	int threshold=13;
    	
    	System.out.println("Process initiated....");
        Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
        Connection con = DriverManager.getConnection("jdbc:monetdb://localhost:50000/demo", "monetdb", "monetdb");
        DatabaseMetaData metadata = con.getMetaData();
        Statement st = con.createStatement();
        
        ResultSet resultSet;
  	  
  	  	resultSet = metadata.getTables(null, null,"masd", null);
  	  	if(resultSet.next())
  	  	{
  	  		st.executeUpdate("drop table masd");
  	  	}
  	  	resultSet = metadata.getTables(null, null,"Header", null);
  	  	if(resultSet.next())
  	  	{
  	  		st.executeUpdate("drop table Header");
  	  	}
  	  	resultSet = metadata.getTables(null, null,"sortHeader", null);
  	  	if(resultSet.next())
  	  	{
  	  		st.executeUpdate("drop table sortHeader");
  	  	}
  	  	resultSet = metadata.getTables(null, null,"extendedfp_table", null);
  	  	if(resultSet.next())
  	  	{
  	  		st.executeUpdate("drop table extendedfp_table");
  	  	}
  	  	resultSet = metadata.getTables(null, null,"finalfp_table", null);
  	  	if(resultSet.next())
  	  	{
  	  		st.executeUpdate("drop table finalfp_table");
  	  	}
  	  	resultSet = metadata.getTables(null, null,"sorted_masd", null);
	  	if(resultSet.next())
	  	{
	  		st.executeUpdate("drop table sorted_masd");
	  	}
        	  
  	  	File file1 = new File("E:/Eclipse/workspace/Vertical_fpgrowth/src/output1.txt");
      	if (!file1.exists()) 
      	{
      			file1.createNewFile();
      			System.out.println("file created");
      	}
      	FileWriter fw = new FileWriter(file1.getAbsoluteFile());
      	PrintWriter bw = new PrintWriter(fw);
        	
        String val_s;
        long endTime,totalTime;
        int line=0,temp; //line is total no. of transactions
        double maxvalue=0;  //maxvalue is length of longest transaction 
        int total_items=0;
		ResultSet rs,rs1;
        int element=0,match=0;
        double val;
        String string;
        int j=0,i;
        
        //counting maximum and lines in file
        File file = new File("E:/Eclipse/workspace/Vertical_fpgrowth/src/mushrooms.txt");
        FileReader reader = new FileReader(file);
        BufferedReader in = new BufferedReader(reader);
        
        while ((string = in.readLine()) != null) 
        {
        	line++;
      	  	String [] tokens = string.split("\\s+"); // splitting each line 
      	  	val=tokens.length; //no of elements in one transaction
      	  	if(val>maxvalue)
      	  		maxvalue=val;
        }
        reader.close();
        in.close(); 
        
        //creating table masd, for vertical storage of transactions
  	  	st.executeUpdate("CREATE TABLE masd(item_id int)");
  	  	
  	  	// Adding maxvalue no of columns - c_0,c_1 etc
   	  	for(i=0;i<line;i++)
   	  		st.executeUpdate("Alter table masd Add column c_"+i+" int");
    	line=0;
   	  	file = new File("E:/Eclipse/workspace/Vertical_fpgrowth/src/mushrooms.txt");
   	  	reader = new FileReader(file);
   	  	in = new BufferedReader(reader);
   	  	
   	  	
   	  	while ((string = in.readLine()) != null) 
   	  	{
   	  		line++;  // No of lines in file ie no of transactions in file
   	  		String [] tokens = string.split("\\s+"); // splitting each line 
   	  		val=tokens.length; //no of elements in one transaction
   	  		for(i=0;i<val;i++) 
   	  		{
   	  			val_s=tokens[i];
   	  			temp=Integer.parseInt(val_s);
	  			rs=st.executeQuery("Select * from masd where item_id="+temp+"");
	  			j=0;
	  			
	  			if(rs.next())//item_id exist in masd
	  			{
	  				for(j=3;j<maxvalue+3;j++)
	  				{
		  				if(rs.getInt(j)==0)
		  				{
		  					st.executeUpdate("UPDATE masd SET c_"+(j-2)+"="+line+" where item_id="+temp+"");
	  						break;
		  				}
	  				}
	  			}
	  			else
	  			{
	  				st.executeUpdate("insert into masd(item_id,c_0) values("+temp+","+line+")");
	  			}
   	  		}
   	  	}
   	  	reader.close();
   	  	in.close();
   	  	
	   	endTime= System.currentTimeMillis();
	    totalTime=endTime-startTime;
	    System.out.println("Time taken for creating masd :"+totalTime+"ms");
	   	  	
   	  	//query for counting total no. items in file
   	  	rs=st.executeQuery("select count(*) from masd");
   	  	if(rs.next())
   	  		total_items = rs.getInt(1);
   	  	//System.out.println(total_items);
   	  	
   	  	//creating table sorted_masd, for vertical storage of transactions
  	  	st.executeUpdate("CREATE TABLE sorted_masd(item_id int)");
   	  	
  	  	// Adding maxvalue no of columns - c_0,c_1 etc
   	  	for(i=0;i<line;i++)
   	  		st.executeUpdate("Alter table sorted_masd Add column c_"+i+" int");
   	  	
   	  	//creating table sortheader
  	  	st.executeUpdate("CREATE TABLE sortheader(item_id int,freq int)");
   	 
	  	int[] temp_masd_array = new int[total_items];
	  	
	  	//startTime = System.currentTimeMillis();
   	  	//for row of masd
   	  	for(i=0,j=line-1;j>=(threshold-1);j--)
   	  	{
   	  		for(int k=0;k<total_items;k++)
   	  			temp_masd_array[k]=0;
   	  		i=0;
   	  		rs=st.executeQuery("Select * from masd where c_"+j+" is not null");
   	  		while(rs.next())
   	  		{
   	  			temp_masd_array[i]=rs.getInt(1);
   	  			i++;
   	  		}
   	  		if(i>1)
   	  			bubblesort(temp_masd_array,i);
   	  		for (int k=0;k<i;k++)
   	  		{
   	  			//System.out.println(temp_masd_array[k]);
   	  			st.executeUpdate("insert into sorted_masd select * from masd where item_id="+temp_masd_array[k]+"");
   	  			st.executeUpdate("delete FROM masd WHERE item_id="+temp_masd_array[k]+"");
   	  			st.executeUpdate("insert into sortheader values("+temp_masd_array[k]+","+(j+1)+")");
   	  		}
   	  	}
   	  	endTime= System.currentTimeMillis();
	    totalTime=endTime-startTime;
	    System.out.println("Time taken for creating sorted_masd and sortheader :"+totalTime+"ms");
   	  	st.executeUpdate("drop table masd");
   	  	
   	  	int newMax=0,newLine=0;
	   	rs=st.executeQuery("Select count(*),max(freq) from sortheader");
	    if(rs.next())
	    {
	    	newMax=rs.getInt(1); // stores no of rows in header table
	    	newLine=rs.getInt(2); // stores maximum support count from header
	    }
	     
	    int max2=10*newMax;
   	  	//creating fp_table (extended)
        st.executeUpdate("CREATE TABLE extendedfp_table(item_id INTEGER,parent varchar("+max2+"),count INTEGER)");
        
        //creating finalfp_table this is actual output file
	  	st.executeUpdate("CREATE TABLE finalfp_table(fp varchar("+max2+"), support INTEGER )");
	  	
	  	int tempHeader[] = new int[newMax];  //for item_id
        int tempHeader1[]=new int [newMax];  //for its corresponding freq
        int id1=0,id=0,j1=0; //j1 is index for tempheader and tempheader1
        String pr,pr1;
        pr1="0";
        
        for(i=1;i<=line;i++)
        {
        	//reassign to 0
   	  		for(int k=0;k<j1;k++)
   	  		{
   	  			tempHeader[k]=0;
   	  			tempHeader1[k]=0;
   	  		}
   	  		
        	//creating string for query
   	  		j1=0;
	        string="";
	        for(int k=0;k<=(i-1);k++)
	        {
	        	if(k==(i-1))
	        		string=string+"c_"+Integer.toString(k)+"="+Integer.toString(i)+" ";
	        	else
	        		string=string+"c_"+Integer.toString(k)+"="+Integer.toString(i)+" or ";
	        }
	        rs=st.executeQuery("Select item_id,freq from sorted_masd natural join sortheader where "+string+"");
	        while(rs.next())
	        {
	           	tempHeader[j1]=rs.getInt(1);//itemid of C_j
	           	tempHeader1[j1]=rs.getInt(2);//"freq"
	           	j1++;	// total no of elements in each transaction
            }
	        //entering data in extended fp_table
            for(int k=0;k<j1;k++)
            {
                	id=tempHeader[k]; //id stores item value
                	if(pr1=="0")
                		pr="null";
                	else 
                		pr=pr1+":"+id1;
                	pr1=pr;
                	st.executeUpdate("INSERT into extendedfp_table VALUES("+id+",'"+pr+"',1)");  
                	id1=id;
            }
	        pr1="0";
        } 
        
        rs=st.executeQuery("Select * from sortheader");
        temp=0; 

        int headeritem[] = new int[newMax];  
        int headerfreq[] = new int[newMax];
        while(rs.next())
        {
        	 headeritem[temp]=rs.getInt("item_id");
        	 headerfreq[temp]=rs.getInt("freq");
        	 temp++;
        }
        
        String sep[]= new String[total_items];
        String parentString[]= new String[total_items*10];//for storing parent(path) from fp_table 
        int l=0,tp=0;
        int rows_twoD=0,y=0;
     	int rows_count2D=0;
     	int yarray[]=new int[line];
     	int twoD[][]=new int[line][newMax];
		int oneD[]=new int[total_items+2];
		int count2D[][]=new int[newMax+2][2];
		int ctl[]=new int[line];
		int reduce=0;
		int element_freq=0;
		String parent_finalfp;
		int v=newMax;
		
		//use newmax in place of totaliteminheader and v, use total_items in place of newMax
		for(int each_element=newMax-1;each_element>=0;each_element-- )//for each element starting from bottom of sortheader table
	      {
	       		  element=headeritem[each_element]; 
	       		  element_freq=headerfreq[each_element];
	       		  l=0;
	       		  rows_twoD=0;
	      	  	  rows_count2D=0;
	      	  	  st.executeUpdate("delete FROM sortheader WHERE item_id="+element+";");
	      	  	  v--;
	       	  	  rs1=st.executeQuery("Select parent,count from extendedfp_table where item_id="+element+" ");
	       	  	  //splitting string
	       	  	  while(rs1.next()) // for each path of element
	       	  	  {
	       	  		  parentString[l]=rs1.getString("parent");
	       	  		  ctl[l]=rs1.getInt("count");
	       	  		  l++; // no of paths for element
	       	  	  }
     	  	  
	       	  	  //creating unsorted twoD and unsorted count2D
	       	  	  for(i=0;i<l;i++)	//for each path
	       	  	  {
	       	  		  for(int counter=0;counter<ctl[i];counter++)//for dup rows
	       	  		  {	
	       	  			  sep=parentString[i].split(":"); //each path
	       	  			  int sepLength=sep.length; //length of path including null
	       	  			  //splitting string completed.
	       	  			  match=0;
	       	  			  y=0;
	       	  			  for(int p=0;p<sepLength;p++) //for individual element in path
	       	  			  {
	       	  				  if(sep[p].compareTo("null")!=0)
	       	  				  {
	       	  					  twoD[rows_twoD][y]=Integer.parseInt(sep[p]);
	       	  					  y++; //stores length of path excluding null, finally
	       	  					  tp=Integer.parseInt(sep[p]);
	       	  					  for(int check=0;check<rows_count2D;check++)
	       	  					  {
	       	  						  if(tp==count2D[check][0])
	       	  						  {
	       	  							  count2D[check][1]++;
	       	  							  match=1;
	       	  						  }
	       	  					  }
	       	  					  if(match==0)
	       	  					  {
	       	  						  	count2D[rows_count2D][0]=tp;
		       	  						count2D[rows_count2D][1]=1;
		       	  						rows_count2D++;
	       	  					  }
	       	  					  match=0; // if match=0 then it means that the item value is not in count2D array
	       	  				  }
	       	  			  }//for ended for indvidual parent
	       	  			  yarray[rows_twoD]=y;
	       	  			  rows_twoD++;
	       	  		  }	
	       	  		  //unsorted count2D and unsortd twoD complete
	       	  	  }
     	  	  
	       	  	  //sorting count2D
	       	  	  if(rows_count2D>1)
	       	  		  merge_srt2D(count2D,0,rows_count2D-1); // sorts on the basis of frequency only
	       	  	  for(int p=0;p<rows_count2D;p++)
	       	  	  {
	       	  		  if(count2D[p][1]<threshold)
     	  			  {
	       	  			  rows_count2D=p; // gives the no of valid rows in count2D
	       	  			  break;
     	  			  }
	       	  	  }
     	  	  
	       	  	  //sorting twoD
	       	  	  int count_oneD=0;
	       	  	  for(int p=0;p<rows_twoD;p++) // for each path
	       	  	  {
	       	  		  count_oneD=0;
	       	  		  for(int p1=0;p1<yarray[p];p1++) //for element in each path
	       	  		  {
	       	  			  for(int p2=0;p2<rows_count2D;p2++)
	       	  			  {
	       	  				  if(count2D[p2][0]==twoD[p][p1])
	       	  				  {
	       	  					  tempHeader1[count_oneD]=count2D[p2][1]; //overwriting with frequency, check code to reassign with 0 
	       	  					  oneD[count_oneD]=twoD[p][p1];// storing valid element of path
	       	  					  match=1;
	       	  					  count_oneD++;
	       	  					  break;
	       	  				  } 
	       	  			  }
	       	  			  if(match==0)
	       	  			  { 
	       	  				  reduce++;
	       	  			  }
	       	  			  match=0;
	       	  		  }
	       	  		  yarray[p]=yarray[p]-reduce; // now stores valid length of path with elements having frequency>=threshold
	       	  		  int tpp=yarray[p]-1;
	       	  		  
	       	  		  if(yarray[p]>1) 	
	          			   merge_srt(oneD,tempHeader1,0,tpp);
	       	  		  reduce=0;
	       	  		  match=0;
	       	  		  for(int p1=0;p1<yarray[p];p1++)
	       	  		  {
	        	  			twoD[p][p1]=oneD[p1]; //copying sorted oneD to twwoD
	       	  		  }
     	  		  }//sorting of twoD complete
     	  	 
	       	  	  parent_finalfp=Integer.toString(element);
	       	  	  
	       	  	  bw.write(parent_finalfp);
	       	  	  bw.write(" #sup ");
	       	  	  bw.write(Integer.toString(element_freq));
	       	  	  bw.printf("\r\n");
	       	  	  total_fp++;
	       	  	  
	       	  	  //System.out.println(parent_finalfp+" #sup "+Integer.toString(element_freq)+"\r\n");
	       	  	  
	       	  	  //recursion starting
	       	  	  recur(count2D,twoD,rows_twoD,yarray,total_items,newMax,rows_count2D,parent_finalfp,threshold,newLine,file1,bw,fw);
     	  }//for completed for each element
     	  
     	  bw.close();
     	  
     	  //inserting data into finalfp_table
     	  FileReader reader1 = new FileReader(file1);
          BufferedReader in1 = new BufferedReader(reader1);
          reader1 = new FileReader(file1);
          in1 = new BufferedReader(reader1);
          while ((string = in1.readLine()) != null) 
          {
             	  String [] tokens = string.split("\\s+");
             	  val=tokens.length;
         		  val_s=tokens[2];
         		  temp=Integer.parseInt(val_s);
         		  st.executeUpdate("INSERT into finalfp_table VALUES('"+tokens[0]+"',"+temp+")"); 
         		  
           }
           System.out.println("Total no of frequent patterns : "+total_fp);
     	   endTime= System.currentTimeMillis();
           totalTime=endTime-startTime;
           System.out.println("Total time taken : "+totalTime);
           
           con.close();
           in.close();
           in1.close();
           System.out.println("Process completed.");
	}              

	public static void bubblesort(int[] input,int len) 
	{	
		int temp;
		for(int i=0;i<len-1;i++)
		{
			for(int j=0;j<len-i-1;j++)
			{
				if(input[j]>input[j+1])
				{
					temp=input[j];
					input[j]=input[j+1];
					input[j+1]=temp;
				}
			}
		}
	}
	
	//merge sort2D
    public static void merge_srt2D(int array[][],int lo, int n)
    { 
	   int low = lo;
	   int high = n;
	   if (low >= high)
	   {  
		   return;
	   }
	
	   int middle = (low + high) / 2;
	   merge_srt2D(array ,low, middle);
	   merge_srt2D(array, middle + 1, high);
	   int end_low = middle;
	   int start_high = middle + 1;
	   while ((low <= end_low) && (start_high <= high))
	   {
	 	  if (array[low][1] >= array[start_high][1]) 
	 	  {
	 		  low++;
	 	  }
	 	  else
	 	  {
	 		  int Temp = array[start_high][1];
	 		  int Temp1 = array[start_high][0];
	 		  for (int k = start_high- 1; k >= low; k--)
	 		  {
	 			  array[k+1][1] = array[k][1];
	 			  array[k+1][0] = array[k][0];
	 		  }
	 		  array[low][1] = Temp;
	 		  array[low][0] = Temp1;
	 		  low++;
	 		  end_low++;
	 		  start_high++;
	 	  }
	   }
    }//end of merge srt2D
	
    //merge sort
    public static void merge_srt(int array[],int array1[],int lo, int n)
    { 
	   int low = lo;
	   int high = n;
	   if (low >= high)
	   {  
		   return;
	   }
	
	   int middle = (low + high) / 2;
	   merge_srt(array,array1 ,low, middle);
	   merge_srt(array,array1, middle + 1, high);
	   int end_low = middle;
	   int start_high = middle + 1;
	   while ((low <= end_low) && (start_high <= high))
	   {
	 	  if (array1[low] >= array1[start_high]) 
	 	  {
	 		  low++;
	 	  }
	 	 
	 	  else
	 	  {
	 		  int Temp = array1[start_high];
	 		  int Temp1 = array[start_high];
	 		  for (int k = start_high- 1; k >= low; k--)
	 		  {
	 			  array1[k+1] = array1[k];
	 			  array[k+1] = array[k];
	 		  }
	 		  array1[low] = Temp;
	 		  array[low] = Temp1;
	 		  low++;
	 		  end_low++;
	 		  start_high++;
	 	  }
	   }
    }//end of merge_srt
    
    ///Recursion
    public static void recur(int count2D[][],int twoD[][],int rows_twoD,int yarray[],int total_items,int newMax,int rows_count2D,String parent_finalfp,int threshold,int newLine, File file1, PrintWriter bw, FileWriter fw ) throws IOException
	{
    	//initializing temp instances
    	int ttwoD[][]=new int[newLine+1][newMax+2];
    	int tcount2D[][]=new int[newMax+2][2];
    	int trows_count2D=0;
    	int trows_twoD=0;
    	int tyarray[] = new int[newLine+1];
	  	int p=0;
	  	int finalmatch=0;
	  	int containing_element[]=new int[total_items*2];
	  	int row=0;
    	int array[]=new int[newMax+2];
    	int larray[]=new int[newMax+2];
    	String sep[]= new String[total_items*2];
	  	for(int rec=rows_count2D-1;rec>=0;rec--)
	  	{
	  		String tparent_finalfp=parent_finalfp;
	  		trows_count2D=0;
	  		trows_twoD=0;
	  		p=0;
	  		finalmatch=0;
	  		String reverse="";
	  		tparent_finalfp=tparent_finalfp+":"+Integer.toString(count2D[rec][0]); // for [2 2], creating 1:2 where element is 1 
	  		sep=tparent_finalfp.split(":");
  	  	int sepLength=sep.length;
  	  	//splitting string completed.
  	  		
  	  	for(int i=sepLength-1;i>=0;i--) //reversing 1:2 to 2:1 for [2 2]
  	  	{
  	  			if(i==sepLength-1)
  	  			reverse=reverse+sep[i];
  	  			else
  	  			reverse=reverse+":"+sep[i];	
  	  	}
	  		
  	  	//writing in output file
			bw.write(reverse);
			bw.write(" #sup ");
			bw.write(Integer.toString(count2D[rec][1]));
			bw.printf("\r\n");
			total_fp++;
			
			//System.out.println(reverse+" #sup "+Integer.toString(count2D[rec][1])+"\r\n");
	  		
	  		int match=0;
	    	//tcount2D create started
	    	for( int i=0;i<rows_twoD;i++) //for each path
	    	{
	    		for(int k=0;k<yarray[i];k++) // for each valid element in path
	    		{
	    			if(twoD[i][k]==count2D[rec][0])
	    			{
	    				containing_element[p]=i;
	    				tyarray[p]=k;
	    				p++;
	    				for(int l=0;l<k;l++)
	    				{
		    				for(int j=0;j<trows_count2D;j++)//finding if item already exists in tcount2D
		    				{
		    					if(twoD[i][l]==tcount2D[j][0])
		    					{
		    						tcount2D[j][1]++;
		    						match=1;
		    						break;
		    					}
		    				}
		    				if(match==0)
		    				{
		    					tcount2D[trows_count2D][0]=twoD[i][l];
		    					tcount2D[trows_count2D][1]=1;
		    					trows_count2D++;
		    				}
		    				match=0;
	    				}
	    			}
	    		}
	    	}// count2D complete
	    	if(trows_count2D>1)
	    		merge_srt2D(tcount2D,0,trows_count2D-1);
	    	for(int l=0;l<trows_count2D;l++)
	    	{
	    		if(tcount2D[l][1]<threshold)
	    		{
	    			trows_count2D=l;
	    			break;
	    		}
	    	}
	        //sorted count2D ended
	    	
	  		// ttwo2D create started
	  		for(int k=0;k<p;k++)
	  		{
	  			row=containing_element[k];
	  			for(int j=0;j<tyarray[k];j++)
	  			{
	  				ttwoD[k][j]=twoD[row][j];
	  			}
	  		}
	  		//ttwoD create completed.
	  		trows_twoD=p;
	  		match=0;
	  		p=0;
	  		
	  		//sorting of ttwoD starting
	  		for(int k=0;k<trows_twoD;k++)
	  		{
	  			for(int j=0;j<tyarray[k];j++)
	  			{
	  				array[j]=ttwoD[k][j];
	  			}
	  			for(int j=0;j<tyarray[k];j++)
	  			{
	  				for(int m=0;m<trows_count2D;m++)
	  				{
	  					if(tcount2D[m][0]==array[j])
	  					{
	  						larray[j]=tcount2D[m][1];
	  						match=1;
	  						break;
	  					}
	  				}
	  				if(match==0)
	  				{
	  					for(int m=j;m<tyarray[k];m++)
	  					{
	  						array[m]=array[m+1];
	  					}
	  					tyarray[k]--;
	  					j--;
	  				}
	  				match=0;
	  			}
	  			if(tyarray[k]>1)
	  				merge_srt(array,larray,0,tyarray[k]-1);
	  			for(int j=0;j<tyarray[k];j++)
	  			{
	  				ttwoD[k][j]=array[j];
	  			}
	  		}//sort ttwoD completed
	  		
	  		//end condition for recursion
	  		for(int m=0;m<trows_twoD;m++)
	  		{
	  			  if(tyarray[m]==0)
	  				  finalmatch=1;
	  			  else
	  			  {
	  				  finalmatch=0;
	  				  break;
	  			  }
	  		}
	  		newLine=tcount2D[0][1];
	  		newMax=trows_count2D;
	  		
	  		if(finalmatch==0)
	  		{
	  			recur(tcount2D,ttwoD,trows_twoD,tyarray,total_items,newMax,trows_count2D,tparent_finalfp,threshold,newLine,file1,bw,fw);
	  		}
	  		  
	  		for(int m=0;m<p;m++)
	  			tyarray[m]--;
	  	}
	 }
}

package kxb162030DatabaseEngine;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class SelectData {
	

	
		
		public static void printRow(String SchemaName, String TableName, ArrayList<Integer> Locations){
			
			for(int i=0;i<Locations.size(); i++)
			{
				try{
					RandomAccessFile tableFile=new RandomAccessFile(SchemaName+"."+TableName+".tbl", "rw");
					//move pointer to location where this row started.
					tableFile.seek(Locations.get(i));
					
					//retrieve the number of values that current row has
					int NumberValuesOfCurrentRow=getNumberOfValuesInRow(Locations.get(i), SchemaName, TableName);
					
					//get current row column names + PRINT
					ArrayList<String> ColumnNamesInThisRow=RetrieveColumnInfo.getColumnNames(SchemaName, TableName);
					for(int j=0; j<NumberValuesOfCurrentRow;j++)
					{
						System.out.print(ColumnNamesInThisRow.get(j)+"\t");
					}
					System.out.println();
					
					//get current row column types
					ArrayList<String> ColumnTypesInThisRow=RetrieveColumnInfo.getColumnTypes(SchemaName, TableName);
					
					
					//get current row Values + PRINT
					for(int k=0; k<NumberValuesOfCurrentRow;k++){
						String currentType=ColumnTypesInThisRow.get(k).toLowerCase().replace("\\s+", "");
						
						if(currentType.equals("byte"))
						{byte val=tableFile.readByte();
						System.out.print(val+" |\t");}
						else if(currentType.equals("short")||currentType.equals("shortint"))
						{Short val=tableFile.readShort();
						System.out.print(val+" |\t");}
						else if(currentType.equals("int"))
						{Integer val=tableFile.readInt();
						System.out.print(val+" |\t");}
						else if(currentType.equals("long")||currentType.equals("longint"))
						{Long val=tableFile.readLong();
						System.out.print(val+" |\t");}
						else if(currentType.equals("float"))
						{Float val=tableFile.readFloat();
						System.out.print(val+" |\t");}
						else if(currentType.equals("double"))
						{Double val=tableFile.readDouble();
						System.out.print(val+" |\t");}
						else if(currentType.equals("datetime"))
						{Long valinlong=tableFile.readLong();
						SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
						Date date=new Date(valinlong);
						String dateinstring=formatter.format(date);
						System.out.print(dateinstring+" |\t");}
						else if(currentType.equals("date"))
						{Long valinlong=tableFile.readLong();
						SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
						Date date=new Date(valinlong);
						String dateinstring=formatter.format(date);
						System.out.print(dateinstring+" |\t");}
						else if(currentType.equals("char")||currentType.equals("varchar"))
						{
							String val="";
							byte length=tableFile.readByte();
							for(int m=0; m<length; m++){
								val+=(char)tableFile.readByte();
							}
							System.out.print(val+" |\t");
						}
						
						
						
					}
					System.out.println("");
					
					
				}catch(Exception e){}
			}
			
		}
		
		
		
		public static Integer getNumberOfValuesInRow(Integer targetlocation,String SchemaName, String TableName){
			try{
				
				RandomAccessFile rowInfo=new RandomAccessFile(SchemaName+"."+TableName+".RowInfo.ndx", "rw");
				int bytesRead=0;
				while(bytesRead<rowInfo.length()){
					int location=rowInfo.readInt();
					bytesRead+=4;
					
					int numberofNumbers=rowInfo.readInt();
					bytesRead+=4;
					if(location==targetlocation)
					{return numberofNumbers;}
				}
				
			}catch(Exception e){System.out.println(e.getMessage());}
			return 0;
		}
		

		public static ArrayList<Integer> getLocations(String SchemaName, String TableName, String ColumnName, String CompValue, Integer JudgeSymble){
			ArrayList<Integer> result=new ArrayList<>();
			
			String TypeName=RetrieveColumnInfo.getColumnType(ColumnName, SchemaName, TableName).toLowerCase();
			
			
			if(TypeName.equals("byte"))
			{
				Byte comp=Byte.parseByte(CompValue);
				if(JudgeSymble==1)
				{result=selectByteEqual(SchemaName, TableName, ColumnName, comp);
				return result;}
				else if(JudgeSymble==2)
				{
					result=selectByteNotEqual(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==3)
				{
					result=selectByteSmaller(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==4)
				{
					result=selectByteLarger(SchemaName, TableName, ColumnName, comp);
					return result;
				}
			}
			else if(TypeName.endsWith("short")||TypeName.endsWith("short"))
			{
				Short comp=Short.parseShort(CompValue);
				if(JudgeSymble==1)
				{result=selectShortEqual(SchemaName, TableName, ColumnName, comp);
				return result;}
				else if(JudgeSymble==2)
				{
					result=selectShortNotEqual(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==3)
				{
					result=selectShortSmaller(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==4)
				{
					result=selectShortLarger(SchemaName, TableName, ColumnName, comp);
					return result;
				}
			}
			else if(TypeName.equals("int"))
			{
				Integer comp=Integer.parseInt(CompValue);
				if(JudgeSymble==1)
				{result=selectIntEqual(SchemaName, TableName, ColumnName, comp);
				return result;}
				else if(JudgeSymble==2)
				{
					result=selectIntNotEqual(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==3)
				{
					result=selectIntSmaller(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==4)
				{
					result=selectIntLarger(SchemaName, TableName, ColumnName, comp);
					return result;
				}
			}
			else if(TypeName.equals("long")||TypeName.equals("longint"))
			{
				Long comp=Long.parseLong(CompValue);
				if(JudgeSymble==1)
				{result=selectLongEqual(SchemaName, TableName, ColumnName, comp);
				return result;}
				else if(JudgeSymble==2)
				{
					result=selectLongNotEqual(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==3)
				{
					result=selectLongSmaller(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==4)
				{
					result=selectLongLarger(SchemaName, TableName, ColumnName, comp);
					return result;
				}
			}
			else if(TypeName.equals("char")||TypeName.equals("varchar"))
			{
				String comp=CompValue;
				if(JudgeSymble==1)
				{result=selectCharEqual(SchemaName, TableName, ColumnName, comp);
				return result;}
				else if(JudgeSymble==2)
				{result=selectCharNotEqual(SchemaName, TableName, ColumnName, comp);
				return result;}
			}
			else if(TypeName.equals("float"))
			{
				Float comp=Float.parseFloat(CompValue);
				if(JudgeSymble==1)
				{result=selectFloatEqual(SchemaName, TableName, ColumnName, comp);
				return result;}
				else if(JudgeSymble==2)
				{
					result=selectFloatNotEqual(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==3)
				{
					result=selectFloatSmaller(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==4)
				{
					result=selectFloatLarger(SchemaName, TableName, ColumnName, comp);
					return result;
				}
			}
			else if(TypeName.equals("double"))
			{
				 Double comp=Double.parseDouble(CompValue);
				if(JudgeSymble==1)
				{result=selectDoubleEqual(SchemaName, TableName, ColumnName, comp);
				return result;}
				else if(JudgeSymble==2)
				{
					result=selectDoubleNotEqual(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==3)
				{
					result=selectDoubleSmaller(SchemaName, TableName, ColumnName, comp);
					return result;
				}
				else if(JudgeSymble==4)
				{
					result=selectDoubleLarger(SchemaName, TableName, ColumnName, comp);
					return result;
				}
			}
			else if(TypeName.equals("date"))
			{
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				
				try{Date date=formatter.parse(CompValue);
				
				long comp=date.getTime();
				
				if(JudgeSymble==1)
				{result=selectLongEqual(SchemaName, TableName, ColumnName, comp);}
				else if(JudgeSymble==2)
				{result=selectLongNotEqual(SchemaName, TableName, ColumnName, comp);}
				else if(JudgeSymble==3)
				{result=selectLongSmaller(SchemaName, TableName, ColumnName, comp);}
				else if(JudgeSymble==4)
				{result=selectLongLarger(SchemaName, TableName, ColumnName, comp);}
				}catch(Exception e){}
			}
			else if(TypeName.equals("datetime"))
			{
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				try{Date date=formatter.parse(CompValue);
				long comp=date.getTime();
				if(JudgeSymble==1)
				{result=selectLongEqual(SchemaName, TableName, ColumnName, comp);}
				else if(JudgeSymble==2)
				{result=selectLongNotEqual(SchemaName, TableName, ColumnName, comp);}
				else if(JudgeSymble==3)
				{result=selectLongSmaller(SchemaName, TableName, ColumnName, comp);}
				else if(JudgeSymble==4)
				{result=selectLongLarger(SchemaName, TableName, ColumnName, comp);}
				}catch(Exception e){}
				
			}
			return result;
		}
		
		public static ArrayList<Integer> selectIntEqual(String SchemaName, String TableName, String ColumnName, Integer comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				int key=indexFile.readInt();//get value stored
				bytesRead+=4;
				if(key==comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectIntLarger(String SchemaName, String TableName, String ColumnName, Integer comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				int key=indexFile.readInt();//get value stored
				bytesRead+=4;
				if(key>comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectIntSmaller(String SchemaName, String TableName, String ColumnName, Integer comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				int key=indexFile.readInt();//get value stored
				bytesRead+=4;
				if(key<comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectIntNotEqual(String SchemaName, String TableName, String ColumnName, Integer comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				int key=indexFile.readInt();//get value stored
				bytesRead+=4;
				if(key!=comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}
		
		public static ArrayList<Integer> selectByteEqual(String SchemaName, String TableName, String ColumnName, byte comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				int key=indexFile.readByte();//get value stored
				bytesRead+=4;
				if(key==(int)comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectByteLarger(String SchemaName, String TableName, String ColumnName, byte comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				int key=indexFile.readByte();//get value stored
				bytesRead+=4;
				if(key>(int)comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectByteSmaller(String SchemaName, String TableName, String ColumnName, byte comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				int key=indexFile.readByte();//get value stored
				bytesRead+=4;
				if(key<(int)comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectByteNotEqual(String SchemaName, String TableName, String ColumnName, byte comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				int key=indexFile.readByte();//get value stored
				bytesRead+=4;
				if(key!=(int)comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectShortEqual(String SchemaName, String TableName, String ColumnName, Short comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Short key=indexFile.readShort();//get value stored
				bytesRead+=2;
				if(key.compareTo(comp)==0)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectShortLarger(String SchemaName, String TableName, String ColumnName, Short comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Short key=indexFile.readShort();//get value stored
				bytesRead+=2;
				if(key>comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectShortSmaller(String SchemaName, String TableName, String ColumnName, Short comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Short key=indexFile.readShort();//get value stored
				bytesRead+=2;
				if(key<comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectShortNotEqual(String SchemaName, String TableName, String ColumnName, Short comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Short key=indexFile.readShort();//get value stored
				bytesRead+=2;
				if(key!=comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectLongEqual(String SchemaName, String TableName, String ColumnName,Long comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Long key=indexFile.readLong();//get value stored
				bytesRead+=8;
				
				if(key.compareTo(comp)==0)//if matches
				{
				int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectLongLarger(String SchemaName, String TableName, String ColumnName,Long comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Long key=indexFile.readLong();//get value stored
				bytesRead+=8;
				if(key>comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectLongSmaller(String SchemaName, String TableName, String ColumnName,Long comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Long key=indexFile.readLong();//get value stored
				bytesRead+=8;
				if(key<comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectLongNotEqual(String SchemaName, String TableName, String ColumnName,Long comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Long key=indexFile.readLong();//get value stored
				bytesRead+=8;
				if(key!=comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectFloatEqual(String SchemaName, String TableName, String ColumnName, Float comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Float key=indexFile.readFloat();//get value stored
				bytesRead+=4;
				if(key.compareTo(comp)==0)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectFloatLarger(String SchemaName, String TableName, String ColumnName, Float comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Float key=indexFile.readFloat();//get value stored
				bytesRead+=4;
				if(key>comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectFloatSmaller(String SchemaName, String TableName, String ColumnName, Float comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Float key=indexFile.readFloat();//get value stored
				bytesRead+=4;
				if(key<comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectFloatNotEqual(String SchemaName, String TableName, String ColumnName, Float comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Float key=indexFile.readFloat();//get value stored
				bytesRead+=4;
				if(key!=comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectDoubleEqual(String SchemaName, String TableName, String ColumnName, Double comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Double key=indexFile.readDouble();//get value stored
				bytesRead+=8;
				if(key.compareTo(comp)==0)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectDoubleLarger(String SchemaName, String TableName, String ColumnName, Double comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Double key=indexFile.readDouble();//get value stored
				bytesRead+=8;
				if(key>comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectDoubleSmaller(String SchemaName, String TableName, String ColumnName, Double comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Double key=indexFile.readDouble();//get value stored
				bytesRead+=8;
				if(key<comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectDoubleNotEqual(String SchemaName, String TableName, String ColumnName, Double comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				Double key=indexFile.readDouble();//get value stored
				bytesRead+=8;
				if(key!=comp)//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectCharEqual(String SchemaName, String TableName, String ColumnName, String comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				String key="";
				//get value length
				byte length=indexFile.readByte();
				bytesRead++;
				for(int k=0; k<length; k++){
					key+=(char)indexFile.readByte();
					bytesRead++;
				}
			
				if(key.equals(comp))//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

		public static ArrayList<Integer> selectCharNotEqual(String SchemaName, String TableName, String ColumnName, String comp){
			
			ArrayList<Integer> locations=new ArrayList<>();//contain locations that matches value
			try{
			RandomAccessFile indexFile=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
			int bytesRead=0;
			while(bytesRead<indexFile.length())
			{
				String key="";
				//get value length
				byte length=indexFile.readByte();
				bytesRead++;
				for(int k=0; k<length; k++){
					key+=(char)indexFile.readByte();
					bytesRead++;
				}
			
				if(!key.equals(comp))//if matches
				{int times=indexFile.readInt();//get number of location record 
				bytesRead+=4;
				for(int i=0; i<times;i++)
				{	//for each record, add it to the locations
					int location=indexFile.readInt();
					bytesRead+=4;
					locations.add(location);
				}	
				}
				else
				{int times=indexFile.readInt();
				bytesRead+=4;
				for(int k=0; k<times;k++)
					{indexFile.readInt();
					bytesRead+=4;}
				}
			}
			
			}catch(Exception e){System.out.println("Error occurs in select : "+e.getMessage());}
			return locations;
		}

	}




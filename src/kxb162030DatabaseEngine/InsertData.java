package kxb162030DatabaseEngine;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map.Entry;
import javax.print.attribute.DateTimeSyntax;
public class InsertData {
	

	
		public static void InsertValues(String SchemaName, String TableName, ArrayList<String> InsertValues){

			//For Normal Value
			ArrayList<String> columnTypes=new ArrayList<>();//receive raw column types=type+length
			ArrayList<String> dataTypes=new ArrayList<>();//receive type name
			ArrayList<Integer> dataLength=new ArrayList<>();//receive type length
			ArrayList<String> Nulable=new ArrayList<>();//receive null / not null information
			ArrayList<String> Key = new ArrayList<>();//receive key instruction
			ArrayList<String> columnNamesList=new ArrayList<>();//receive column nmaes
			
			/**
			 * Get Columns and Constraints 
			 */
			try{
				RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
				int bytesRead=0;
				
				while(bytesRead<columnsTableFile.length()){
					
					String schemaNameString="";
					String tableNameString="";
					String columnNameString="";
					Integer positionInteger=0;
					String columnTypeString="";
					String NulableString="";
					String columnKeyString="";
				
					byte namelength=columnsTableFile.readByte();//read schema name length
					bytesRead++;
					for(int i=0 ; i<namelength; i++)
					{schemaNameString+=(char)columnsTableFile.readByte();
					bytesRead++;}//read schema name
					
	//##############schema name doesn't match########################################
					if(!schemaNameString.equals(SchemaName))
					{
					byte namelength2=columnsTableFile.readByte();//read table length
					bytesRead++;
					for(int i=0 ; i<namelength2; i++)//read table name
					{tableNameString+=(char)columnsTableFile.readByte();
					bytesRead++;}
					
					byte namelength3=columnsTableFile.readByte();//read column length
					bytesRead++;
					for(int i=0 ; i<namelength3; i++)//read column name
					{columnNameString+=(char)columnsTableFile.readByte();
					bytesRead++;}
					
					positionInteger=columnsTableFile.readInt();// read ordination rank
					bytesRead+=4;
					
					byte namelength4=columnsTableFile.readByte();//read read column type(raw) length
					bytesRead++;
					for(int i=0 ; i<namelength4; i++)
					{columnTypeString+=(char)columnsTableFile.readByte();//read read column type(raw)
					bytesRead++;}
					
					byte namelength5=columnsTableFile.readByte();//read null status length
					bytesRead++;
					for(int i=0 ; i<namelength5; i++)//read null status
					{NulableString+=(char)columnsTableFile.readByte();
					bytesRead++;}
					
					byte namelength6=columnsTableFile.readByte();//read key length
					bytesRead++;
					for(int i=0 ; i<namelength6; i++)//read key
					{columnKeyString+=(char)columnsTableFile.readByte();
					bytesRead++;}
					}
					
	//##############schema name matches############################################
					else
					{
						byte namelength2=columnsTableFile.readByte();//read table name length
						bytesRead++;
						for(int i=0 ; i<namelength2; i++)//read table name
						{tableNameString+=(char)columnsTableFile.readByte();
						bytesRead++;}
						
		//table name doesn't match################################################
						if(!tableNameString.equals(TableName)){
						byte namelength3=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength3; i++)//read column name
						{columnNameString+=(char)columnsTableFile.readByte();
						bytesRead++;}
						
						positionInteger=columnsTableFile.readInt();
						bytesRead+=4;//read cordination rank
						
						byte namelength4=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength4; i++)//read column type
						{columnTypeString+=(char)columnsTableFile.readByte();
						bytesRead++;}
						
						byte namelength5=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength5; i++)//read null status
						{NulableString+=(char)columnsTableFile.readByte();
						bytesRead++;}
						
						byte namelength6=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength6; i++)//read key status
						{columnKeyString+=(char)columnsTableFile.readByte();
						bytesRead++;}
						}
						
		//TABLE NAME MATCHES!!! #############################################
						else
							{
							//System.out.println("Everything is perfect");
							byte namelength3=columnsTableFile.readByte();
							bytesRead++;
							for(int i=0 ; i<namelength3; i++)//read column name
							{columnNameString+=(char)columnsTableFile.readByte();
							bytesRead++;}
							columnNamesList.add(columnNameString);//add column name into COLUMN_NAME list
							
							
							positionInteger=columnsTableFile.readInt();
							bytesRead+=4;//read cordination rank
				//!!!READ COLUMN TYPE (NAME+LENGTH)			
							byte namelength4=columnsTableFile.readByte();
							bytesRead++;
							for(int i=0 ; i<namelength4; i++)//read column type
							{columnTypeString+=(char)columnsTableFile.readByte();
							bytesRead++;}
							columnTypes.add(columnTypeString);//add raw column type (name+length) into type list
							dataTypes.add(parsingType(columnTypeString));//parsing type name into list
							dataLength.add(parsingTypeLength(columnTypeString));//parsing type length into list
							
							
							byte namelength5=columnsTableFile.readByte();
							bytesRead++;
							for(int i=0 ; i<namelength5; i++)//read null status
							{NulableString+=(char)columnsTableFile.readByte();
							bytesRead++;}
							Nulable.add(NulableString);//add null status into list
							
							
							byte namelength6=columnsTableFile.readByte();
							bytesRead++;
							for(int i=0 ; i<namelength6; i++)//read key status
							{columnKeyString+=(char)columnsTableFile.readByte();
							bytesRead++;}
							Key.add(columnKeyString);//add key status into list
							}
					}
					
				}
			}catch(Exception e){System.out.println("Error Occurs In Retrieve Constraints: "+e.getMessage());}
			
			if(columnNamesList.size()==0)
			{System.out.println("no such column found!");
			return;}
	//#####################################BASIC INFORMATION RETRIEVAL COMPLETED###################################################################### 
			
			
	//##################################### CHECK VALIDITY OF INSERT VALUES#########################################
			
	boolean valid=ValuesChecking(InsertValues, columnNamesList, Nulable, dataTypes, dataLength, Key, SchemaName, TableName);

	if(!valid)
	{	System.out.println("Your Input Is Not Valid. Insert Denied. Please Try Again.");
	return;}//if the input is not valid. Insert action stops

	//Start Insertion####################################################################################################
			try{
				//get row position
				RandomAccessFile tableFile=new RandomAccessFile(SchemaName+"."+TableName+".tbl", "rw");
				int pointerBeforeInsertion=(int)tableFile.length();
				
				//create new index file to store locations where each row started and number of values in each row
				RandomAccessFile  rowInformation=new RandomAccessFile(SchemaName+"."+TableName+"."+"RowInfo.ndx", "rw");
				long infoPointer=rowInformation.length();
				rowInformation.seek(infoPointer);
				
				
				
				
				int numValues=InsertValues.size();
				
				//write initial location and number of values
				rowInformation.writeInt(pointerBeforeInsertion);
				rowInformation.writeInt(numValues);}catch(Exception e){System.out.println("Error In RowInfo");}
				
	//##################### Insert Values ######################################################################			
			try{	
				RandomAccessFile tableFile=new RandomAccessFile(SchemaName+"."+TableName+".tbl", "rw");
				int currentRow=(int)tableFile.length();//get initial row position
				
			for(int i=0; i<InsertValues.size(); i++)
				{
					//get current value in string
					String currentValueInString=InsertValues.get(i);
					System.out.print("value: "+currentValueInString+"\t");
					//get current column name
					String currentColumnName=columnNamesList.get(i);
					System.out.print("column: "+currentColumnName+"\t");
					//connect column name index file
					RandomAccessFile columnIndexFile = new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
					//get current data type
					String CurrentTypeName=dataTypes.get(i).toLowerCase();
					System.out.print("type name: "+CurrentTypeName+"\n");
					
					if(CurrentTypeName.equals("byte"))
					{
						//0.convert string to current type
						Byte currentValueConverted=Byte.parseByte(currentValueInString);
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						
						//2. start to write into table file
						tableFile.writeByte(currentValueConverted);
						
						//write into index file
						TreeMap<Byte, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							Byte key=columnIndexFile.readByte();//read key, i.e. the value
							bytesRead1++;
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
							
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
							
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<Byte,ArrayList<Integer>> entry : columnIndex.entrySet())
						{Byte key = entry.getKey();
						columnIndexFile2.writeByte(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
					
					
					
					else if(CurrentTypeName.equals("int"))
					{
						//0.convert string to current type
						Integer currentValueConverted=Integer.parseInt(currentValueInString);
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						//2. start to write into table file
						tableFile.writeInt(currentValueConverted);
						
						//write into index file
						TreeMap<Integer, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							Integer key=columnIndexFile.readInt();//read key, i.e. the value/short
							bytesRead1+=4;
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
							
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
							
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<Integer,ArrayList<Integer>> entry : columnIndex.entrySet())
						{Integer key = entry.getKey();
						columnIndexFile2.writeInt(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
					
					
					else if(CurrentTypeName.equals("shortint") || CurrentTypeName.equals("short"))
					{
						//0.convert string to current type
						Short currentValueConverted=Short.parseShort(currentValueInString);
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						//2. start to write into table file
						tableFile.writeShort(currentValueConverted);
						
						//write into index file
						TreeMap<Short, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							short key=columnIndexFile.readShort();//read key, i.e. the value/short
							bytesRead1+=2;
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
							
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
							
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<Short,ArrayList<Integer>> entry : columnIndex.entrySet())
						{Short key = entry.getKey();
						columnIndexFile2.writeShort(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
					
					
					else if(CurrentTypeName.equals("longint") || CurrentTypeName.equals("long"))
					{
						//0.convert string to current type
						long currentValueConverted=Long.parseLong(currentValueInString);
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						//2. start to write into table file
						tableFile.writeLong(currentValueConverted);
						
						//write into index file
						TreeMap<Long, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							long key=columnIndexFile.readLong();//read key, i.e. the value
							bytesRead1+=8;
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
							
							
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
							
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<Long,ArrayList<Integer>> entry : columnIndex.entrySet())
						{Long key = entry.getKey();
						columnIndexFile2.writeLong(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
					
					
					else if(CurrentTypeName.equals("char")||CurrentTypeName.equals("varchar"))
					{
						//0.convert string to current type
						String currentValueConverted=currentValueInString;
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						//2. start to write into table file
					
						tableFile.writeByte(currentValueConverted.length());
						tableFile.writeBytes(currentValueConverted);
						
						//write into index file
						TreeMap<String, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							String key="";
							byte length=columnIndexFile.readByte();
							bytesRead1++;
							for(int x=0; x<length;x++)
							{key+=(char)columnIndexFile.readByte();
							bytesRead1++;}
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
							//check primary key constraint
							
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
						
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<String,ArrayList<Integer>> entry : columnIndex.entrySet())
						{String key = entry.getKey();
						columnIndexFile2.writeByte(key.length());
						columnIndexFile2.writeBytes(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
					
					
					else if(CurrentTypeName.equals("float"))
					{
						//0.convert string to current type
						float currentValueConverted=Float.parseFloat(currentValueInString);
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						//2. start to write into table file
						tableFile.writeFloat(currentValueConverted);
						
						//write into index file
						TreeMap<Float, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							Float key=columnIndexFile.readFloat();//read key, i.e. the value
							bytesRead1+=4;
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
						
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
							
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<Float,ArrayList<Integer>> entry : columnIndex.entrySet())
						{Float key = entry.getKey();
						columnIndexFile2.writeFloat(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
					
					
					
					else if(CurrentTypeName.equals("double"))
					{
						//0.convert string to current type
						Double currentValueConverted=Double.parseDouble(currentValueInString);
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						//2. start to write into table file
						tableFile.writeDouble(currentValueConverted);
						
						//write into index file
						TreeMap<Double, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							Double key=columnIndexFile.readDouble();//read key, i.e. the value
							bytesRead1+=8;
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
						
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
							
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<Double,ArrayList<Integer>> entry : columnIndex.entrySet())
						{Double key = entry.getKey();
						columnIndexFile2.writeDouble(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
					
					
					else if(CurrentTypeName.equals("datetime"))
					{
						SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
						java.util.Date date=formatter.parse(currentValueInString);
						
						//0.convert string to current type
						long currentValueConverted=date.getTime();
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						//2. start to write into table file
						tableFile.writeLong(currentValueConverted);
						
						//write into index file
						TreeMap<Long, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							long key=columnIndexFile.readLong();//read key, i.e. the value
							bytesRead1+=8;
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
							
							
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
							
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<Long,ArrayList<Integer>> entry : columnIndex.entrySet())
						{Long key = entry.getKey();
						columnIndexFile2.writeLong(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
					
					else if(CurrentTypeName.equals("date")) {
						SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
						java.util.Date date=formatter2.parse(currentValueInString);
						
						//0.convert string to current type
						long currentValueConverted=date.getTime();
						//1.table file pointer
						long pointer=tableFile.length();
						tableFile.seek(pointer);
						//2. start to write into table file
						tableFile.writeLong(currentValueConverted);
						
						//write into index file
						TreeMap<Long, ArrayList<Integer>> columnIndex = new TreeMap<>();//add the addresses into key specified tree
						//1.read index file into treeMap
						
						int bytesRead1=0;//tracking how many bytes read
						while(bytesRead1<columnIndexFile.length()){
							ArrayList<Integer> addresses=new ArrayList<>();//address list, contain address info
							long key=columnIndexFile.readLong();//read key, i.e. the value
							bytesRead1+=8;
							
							int times=columnIndexFile.readInt();//read how many the same values exist
							bytesRead1+=4;
							for(int k=0; k<times; k++)//read addresses to a list
							{
								int address=columnIndexFile.readInt();
								addresses.add(address);
								bytesRead1+=4;
							}
							
							
							columnIndex.put(key, addresses);
						}
						//2.add new record into treeMap
							//key contained?
						boolean keyContained=columnIndex.containsKey(currentValueConverted);
							//if contained
						if(keyContained){
							//check primary key constraint
							
							
								//get new array
								ArrayList<Integer> AddressOriginal=columnIndex.get(currentValueConverted);
								AddressOriginal.add(currentRow);
								//insert address array to the tree_Map
								columnIndex.put(currentValueConverted, AddressOriginal);
							
						}
						//if not contained
						else{
							ArrayList<Integer> newAddressList=new ArrayList<>();
							newAddressList.add(currentRow);
							columnIndex.put(currentValueConverted, newAddressList);
						}
							
						//3.rewrite index file
						RandomAccessFile columnIndexFile2=new RandomAccessFile(SchemaName+"."+TableName+"."+currentColumnName+".ndx", "rw");
						for(Entry<Long,ArrayList<Integer>> entry : columnIndex.entrySet())
						{Long key = entry.getKey();
						columnIndexFile2.writeLong(key);
						ArrayList<Integer> AddressUpdated=entry.getValue();
						int newLength=AddressUpdated.size();
						columnIndexFile2.writeInt(newLength);
						for(int l=0;l<newLength;l++){
							columnIndexFile2.writeInt(AddressUpdated.get(l));
						}}
					}
				}
			}catch(Exception e){}
			System.out.println("1 row inserted into "+ TableName);
			
		
			
			
		}
		


		public static boolean ValuesChecking(ArrayList<String> InsertValues, ArrayList<String> ColumnNames, ArrayList<String> Nulable,ArrayList<String> ColumnTypeName, ArrayList<Integer> ColumnTypeLength, ArrayList<String> keyList, String SchemaName, String TableName){
		
			//value length > column number
			if(InsertValues.size()>ColumnNames.size())
			{System.out.println("Error: The Number of Insert Values Exceeds Column Number.\nTable---"+TableName+" Has "+ColumnNames.size()+" Columns.\n You Inserted "+InsertValues.size()+"Values.");
			return false;}
			
			//check not null constraint
			else if(InsertValues.size()<ColumnNames.size())
			{int NumInsert=InsertValues.size();
			int NumColumn=ColumnNames.size();
				for(int i=NumInsert; i<NumColumn; i++)
				{String NullStatus=Nulable.get(i).toLowerCase();
					if(NullStatus.equals("no"))
					{System.out.println("Not Null Value Missing");return false;}
				}	
			}
			
			//check key constraint
			for(int i=0; i<InsertValues.size(); i++)
			{if(keyList.get(i).toLowerCase().equals("pri"))
				{
				String currentType=ColumnTypeName.get(i).toLowerCase();
				boolean duplicate=valueDuplicate(InsertValues.get(i), currentType, SchemaName, TableName, ColumnNames.get(i));
				if(duplicate)
				{System.out.println("Key Duplicated");return false;}
				}
			}
			
			//type constraint
			for(int i=0; i<InsertValues.size(); i++)
			{
				String currentType=ColumnTypeName.get(i).toLowerCase();
				boolean typeViolation=TypeConstraint(InsertValues.get(i), currentType);
				if(typeViolation)
				{System.out.println("Type Constraint Violation");return false;}
			}
			
			//length constraint
			for(int i=0; i<InsertValues.size(); i++)
			{
				String currentType=ColumnTypeName.get(i).toLowerCase();
				Integer currentLengthConstraint=ColumnTypeLength.get(i);
				int currentValueLeng=InsertValues.get(i).length();
				if(currentType.equals("char"))
				{System.out.println("cahr"+InsertValues.size());
					if(currentValueLeng!=currentLengthConstraint)
					{System.out.println("Wrong Char Length. Data Length Defined="+currentLengthConstraint);
					return false;
					}
					}
				else if(currentType.equals("varchar"))
				{
					if(currentLengthConstraint<currentValueLeng)
					{System.out.println("Wrong Varchar Length. Data Length Defined="+currentLengthConstraint);
					return false;}
					}
					
			}
			
			return true;	
		}
		
		
		public static boolean valueDuplicate(String currentValue, String currentType, String SchemaName, String TableName, String ColumnName){
			
			try{
				RandomAccessFile column=new RandomAccessFile(SchemaName+"."+TableName+"."+ColumnName+".ndx", "rw");
				
				if(currentType.equals("byte"))//if type=byte
					{int bytesRead=0; 
					
					while(bytesRead<column.length())
					{	//read value in store
						byte currentByte=Byte.parseByte(currentValue);
						byte record=column.readByte();
						bytesRead++;
						//campare value
					if(record==currentByte)
						return true;
					//read record times
					int times=column.readInt();
					bytesRead+=4;
					//read locations
					for(int i=0; i<times; i++)
					{column.readInt();
					bytesRead+=4;}
						
					}
					
					}
				
				else if(currentType.equals("short")||currentType.equals("shortint"))//if type=short
				{int bytesRead=0; 
				
				while(bytesRead<column.length())
				{Short currentShort=Short.parseShort(currentValue);
				Short record=column.readShort();
				bytesRead+=2;
				if(record==currentShort)
					return true;
				//read record times
				int times=column.readInt();
				bytesRead+=4;
				//read locations
				for(int i=0; i<times; i++)
				{column.readInt();
				bytesRead+=4;}
					
				}
				
				}
				
				else if(currentType.equals("int"))//if type=integer
				{int bytesRead=0; 
				
				while(bytesRead<column.length())
				{int currentInt=Integer.parseInt(currentValue);
				int record=column.readInt();
				bytesRead+=4;
				if(record==currentInt)
					return true;
				//read record times
				int times=column.readInt();
				bytesRead+=4;
				//read locations
				for(int i=0; i<times; i++)
				{column.readInt();
				bytesRead+=4;}
					
				}
				
				}
				
				else if(currentType.equals("long")||currentType.equals("longint"))//if type=long
				{int bytesRead=0; 
				
				while(bytesRead<column.length())
				{long currentlong=Long.parseLong(currentValue);
				Long record=column.readLong();
				bytesRead+=8;
				if(record==currentlong)
					return true;
				//read record times
				int times=column.readInt();
				bytesRead+=4;
				//read locations
				for(int i=0; i<times; i++)
				{column.readInt();
				bytesRead+=4;}
					
				}
				
				}
				
				else if(currentType.equals("double"))//if type=double
				{int bytesRead=0; 
				
				while(bytesRead<column.length())
				{Double currentlong=Double.parseDouble(currentValue);
				Double record=column.readDouble();
				bytesRead+=8;
				if(record==currentlong)
					return true;
				//read record times
				int times=column.readInt();
				bytesRead+=4;
				//read locations
				for(int i=0; i<times; i++)
				{column.readInt();
				bytesRead+=4;}
					
				}
				
				}
				
				else if(currentType.equals("float"))//if type=float
				{int bytesRead=0; 
				
				while(bytesRead<column.length())
				{float currentfloat=Float.parseFloat(currentValue);
				Float record=column.readFloat();
				bytesRead+=4;
				if(record==currentfloat)
					return true;
				//read record times
				int times=column.readInt();
				bytesRead+=4;
				//read locations
				for(int i=0; i<times; i++)
				{column.readInt();
				bytesRead+=4;}
					
				}
				
				}
				
				else if(currentType.equals("date")||currentType.equals("datetime"))//if type=double
				{int bytesRead=0; 
				
				while(bytesRead<column.length())
				{Long currentDate=Long.parseLong(currentValue);
				Long record=column.readLong();
				bytesRead+=8;
				if(record==currentDate)
					return true;
				//read record times
				int times=column.readInt();
				bytesRead+=4;
				//read locations
				for(int i=0; i<times; i++)
				{column.readInt();
				bytesRead+=4;}
					
				}
				
				}
				
				else if(currentType.equals("char")||currentType.equals("varchar"))
				{
						int bytesRead=0;
						while(bytesRead<column.length())
						{
							String record="";
							int recordLength=column.readByte();
							bytesRead++;
							for(int i=0; i<recordLength; i++)
							{
								record+=(char)column.readByte();
								bytesRead++;
							}
							if(record.equals(currentValue))
							{return true;}
							//read record times
							int times=column.readInt();
							bytesRead+=4;
							//read locations
							for(int i=0; i<times; i++)
							{column.readInt();
							bytesRead+=4;}
								
							}
						}
					
				
				
				else {
					return false;
				}
				
				
				}catch(Exception e){e.getMessage();return false;}
			
			return false;
		}
		
		public static boolean TypeConstraint(String currentValue, String currentType){
			
			if(currentType.equals("byte")){
				try{Byte valueConverted=Byte.parseByte(currentValue);
				return false;}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A Byte Type!");return true;}
			}
			
			else if(currentType.equals("short")||currentType.equals("shortint")){
				try{Short valueConverted=Short.parseShort(currentValue);
				return false;}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A Short Type!");return true;}
			}
			
			else if(currentType.equals("int")){
				try{Integer valueConverted=Integer.parseInt(currentValue);
				return false;}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A Integer Type!");return true;}
			}
			
			else if(currentType.equals("long")||currentType.equals("longint")){
				try{Long valueConverted=Long.parseLong(currentValue);
				return false;}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A Long Type!");return true;}
			}
			
			else if(currentType.equals("float")){
				try{Float valueConverted=Float.parseFloat(currentValue);
				return false;}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A Float Type!");return true;}
			}
			
			else if(currentType.equals("double")){
				try{Double valueConverted=Double.parseDouble(currentValue);
				return false;}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A Double Type!");return true;}
			}
			
			else if(currentType.equals("datetime")){
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				try{
					java.util.Date datetime=formatter.parse(currentValue);
					return false;
				}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A DATETIME Type!");return true;}
			}
			
			else if(currentType.equals("date")){
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				try{
					java.util.Date datetime=formatter.parse(currentValue);
					return false;
				}catch(Exception e){System.out.println("Value: "+currentValue+"  Is Not A DATE Type!");return true;}
			}
			
			else 
				return false;
			
			
		}
		
		/*
		 * Extract name and length of constraints*/
		public static String parsingType(String rawType){
			if(rawType.contains("(")){
			String type=rawType.substring(0, rawType.indexOf('('));
			return type;}
			else return rawType;
		}
		
		public static Integer parsingTypeLength(String rawType){
			String typename=parsingType(rawType).toLowerCase();
			if(typename.equals("char") || typename.equals("varchar")){
			String temp = rawType.substring(rawType.indexOf('(')+1,rawType.indexOf(')')).replaceAll("\\s+", "");
			Integer typeLength=Integer.parseInt(temp);
			return typeLength;}
			else
			{
				if(typename.equals("byte"))
					return 1;
				else if(typename.equals("short")||typename.equals("shortint"))
					return 2;
				else if(typename.equals("int") || typename.equals("float"))
					return 4;
				else if(typename.equals("double") || typename.equals("longint") || typename.equals("long") ||typename.equals("datetime") ||typename.equals("date") )
					return 8;
				else 
					return 0;
			}
		}	

	}




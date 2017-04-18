package kxb162030DatabaseEngine;

import java.io.RandomAccessFile;
import java.util.ArrayList;

public class CreateTable {
static int page_size=512;
static String FILEPATH="information_schema.table.tbl";

public static void createTable(String SchemaName, String TableName, ArrayList<String> columnNames, ArrayList<String> columnTypes, ArrayList<String> columnNulable, ArrayList<String> columnKey)
{
	Integer columnnumber=columnNames.size();
	long columnNumber=columnnumber.longValue();//convert column numbers into Long type
	
	if(checkForTableExistance(TableName, SchemaName) )
	{System.out.println("Table '"+TableName+"' Has Already Exist In Schema '"+SchemaName+"'.");}
	else{
	//create a table file named "schema_name.table_name.tbl"
	try{
		RandomAccessFile newtable = new RandomAccessFile(SchemaName+"."+TableName+".tbl", "rw");
	}catch(Exception e){System.out.println("Error Occurs IN Creating New Table: "+e.getMessage());}
	
	//add table into schema_information_table table
	try{
		RandomAccessFile tablesTableFile = new RandomAccessFile(FILEPATH, "rw");
		//move pointer to the end
		long currentLength=tablesTableFile.length();
		//write information
		tablesTableFile.seek(currentLength);
		tablesTableFile.writeByte(SchemaName.length());
		tablesTableFile.writeBytes(SchemaName);//schema name
		tablesTableFile.writeByte(TableName.length());
		tablesTableFile.writeBytes(TableName);//table name
		tablesTableFile.writeLong(0);//table rows, initialized with 0
	}catch(Exception e){System.out.println("Error Occurs In Iserting table information Into Information_schema"+e.getLocalizedMessage());}
	
	//add columns into schema_information_column table
	try{
		RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
		Long currentLength=columnsTableFile.length();
		columnsTableFile.seek(currentLength);
	for(int i=0; i<columnnumber; i++){
		String CurrentColumnName=columnNames.get(i);//get current column name from column Name list
		String CurrentColumnType=columnTypes.get(i);//get current column type from column Type list
		String CurrentColumnKey=columnKey.get(i);//get current column name from column name list
		String CurrentColumnNulable=columnNulable.get(i);//get current column null/not null from null status list
		
		String IsNulable = "YES";
		String Key="";
		if(columnNulable.get(i).toLowerCase().equals("notnull"))
		IsNulable = "NO";
		if(columnKey.get(i).toLowerCase().equals("primarykey"))
		{Key="PRI";
		IsNulable = "NO";}
		
		int temp=i+1;
		columnsTableFile.writeByte(SchemaName.length());
		columnsTableFile.writeBytes(SchemaName);//column schema
		columnsTableFile.writeByte(TableName.length());
		columnsTableFile.writeBytes(TableName);//column table name
		columnsTableFile.writeByte(CurrentColumnName.length());
		columnsTableFile.writeBytes(CurrentColumnName);//column name
		columnsTableFile.writeInt(temp);//ordinal position
		columnsTableFile.writeByte(CurrentColumnType.length());
		columnsTableFile.writeBytes(CurrentColumnType);//column type
		columnsTableFile.writeByte(IsNulable.length());
		columnsTableFile.writeBytes(IsNulable);//is nulable
		columnsTableFile.writeByte(Key.length());
		columnsTableFile.writeBytes(Key);//column key
		
		//create Index File for EACH COLUMN!!!!!
		RandomAccessFile newfile = new RandomAccessFile(SchemaName+"."+TableName+"."+CurrentColumnName+".ndx", "rw");
		
	}
	}catch(Exception e){System.out.println("Error Occurs In Inserting Into schema_information_column table: "+e.getLocalizedMessage());}
}
}

public static boolean checkForTableExistance(String tableName, String schemaName) {
	// TODO Auto-generated method stub
	
	boolean found=false;
	
	try{
		RandomAccessFile tablesTableFile = new RandomAccessFile(FILEPATH, "rw");
		int bytesRead=0;
		
			while(bytesRead<tablesTableFile.length()){
			
			//find schema with given schema name
			String potentialSchemaNameString="";
			//read schema name length
			byte schemaLength=tablesTableFile.readByte();
			bytesRead++;
			//read schema name char by char
			for(int i=0; i<schemaLength; i++)
			{potentialSchemaNameString+=(char)tablesTableFile.readByte();
			bytesRead++;}
			//compare schema name read to the one given
			//if match
			if(potentialSchemaNameString.equals(schemaName))
			{
				//read table name
				String potentialTableNameString="";
				//read table name length
				byte nameLength=tablesTableFile.readByte();
				bytesRead++;
				
				//read table name
				for(int i=0; i<nameLength; i++)
				{
					potentialTableNameString+=(char)tablesTableFile.readByte();
					bytesRead++;
				}
				//print table name
				if(potentialTableNameString.equals(tableName))
				{found=true;
				break;}
			}
			//If not match, continue reading
			else
			{
				byte nameLength=tablesTableFile.readByte();
				bytesRead++;
				for(int i=0; i<nameLength; i++)
				{
					tablesTableFile.readByte();
					bytesRead++;
				}
			}
			//read table rows
			tablesTableFile.readLong();
			bytesRead+=8;
			}
	}catch(Exception e){System.out.println("Error Occur In Checking Table Existance "+e.getMessage());}
	
	
	return found;
	
}
	
}

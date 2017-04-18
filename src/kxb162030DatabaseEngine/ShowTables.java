package kxb162030DatabaseEngine;

import java.io.RandomAccessFile;

public class ShowTables {
	static final String FILEPATH = "information_schema.table.tbl";
	public static void showTables(String schemaName){
		//Has a schema chosen?
		//System.out.println("schema input : " +schemaName);
		if(schemaName.equals(""))
		{System.out.println("Please Choose Schema First !");}
		
		//If chose correct schema
		else{
			System.out.println("TABLE_NAMES\n	------|-------------|------	");
		try{
			int bytesRead=0;
			//read information from inforamtion_schema.table
			RandomAccessFile tablesTableFile = new RandomAccessFile(FILEPATH, "rw");
			
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
				//System.out.println("Schemas : "+potentialSchemaNameString);
				if(potentialSchemaNameString.equals(schemaName))
				{
				//	System.out.println("Schemas : "+potentialSchemaNameString);
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
					
					System.out.println(potentialTableNameString);
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
				}catch(Exception e){System.out.println("Error Occurs In Showing Tables: "+e.getMessage());}
		}
	}
}

package kxb162030DatabaseEngine;

import java.io.RandomAccessFile;

public class DropTable {
	public static boolean dropTable(String SchemaName, String TableName){

		boolean found=false;
		
		try{
			RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema.table.tbl", "rw");
			int bytesRead=0;
			
				while(bytesRead<tablesTableFile.length()){
				Long pointerSchema=tablesTableFile.getFilePointer();
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
				if(potentialSchemaNameString.equals(SchemaName))
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
					if(potentialTableNameString.equals(TableName))
					{found=true;
					//System.out.println(potentialTableNameString);
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
		
		System.out.println(TableName+" dropped!");
		return found;	
	
	
	
	
	
	
	
}

}

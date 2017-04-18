package kxb162030DatabaseEngine;

import java.io.RandomAccessFile;

public class CreateScehma {
	static final String FILEPATH = "information_schema.schemata.tbl";

	public static void CreateSchema(String SchemaName){
		//when a new schema is created, schema name will be listed in the schema information. schema table
		try{
			
		RandomAccessFile schemataTableFile = new RandomAccessFile(FILEPATH, "rw");
		long currentLength=schemataTableFile.length();
		schemataTableFile.seek(currentLength);
		schemataTableFile.writeByte(SchemaName.length());
		schemataTableFile.writeBytes(SchemaName);
		}catch(Exception e){System.out.println("Error Occurs in "+e.getMessage());};
		System.out.println("New Schema '"+SchemaName+"' Has Benn Created!");
	}

}

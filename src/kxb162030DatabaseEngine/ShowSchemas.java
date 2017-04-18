package kxb162030DatabaseEngine;

import java.io.RandomAccessFile;

public class ShowSchemas {
	static final String FILEPATH = "information_schema.schemata.tbl";
	public static void showSchemas(){
	System.out.println("SCHEMAS\n -------|---------------|--------");
	try{
		RandomAccessFile schemataTableFile = new RandomAccessFile(FILEPATH, "rw");	
		int bytesRead=0;
		while(bytesRead<schemataTableFile.length()){
			byte varcharlength = schemataTableFile.readByte();
			bytesRead++;
			for(int i=0; i<varcharlength; i++){
				System.out.print((char)schemataTableFile.readByte());
				bytesRead++;
			}
			System.out.println();
		}
	}
	catch(Exception e){System.out.println("Error Occurs In Show Schemas: "+e.getMessage());}
}
}

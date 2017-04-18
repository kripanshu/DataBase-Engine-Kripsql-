package kxb162030DatabaseEngine;

import java.io.RandomAccessFile;

public class UseSchema {
	static final String FILEPATH = "information_schema.schemata.tbl";
	//Judge whether the schema exist in information schema[string x here is the result after parsing]
		public static boolean useSchema(String x){
			
			boolean found=false;
			try{
			
			RandomAccessFile schemataTableFile = new RandomAccessFile(FILEPATH, "rw");
			byte bytesRead=0;
			
			while(bytesRead<schemataTableFile.length()){
				String tempSchemaNameString="";
				byte varcharlength=schemataTableFile.readByte();
				bytesRead++;
				for(int i=0; i<varcharlength; i++){
					tempSchemaNameString+=(char)schemataTableFile.readByte();
					bytesRead++;
				}
				if(tempSchemaNameString.equals(x)){
					found=true;
					break;
				}
				
			}
			
			}catch(Exception e){System.out.println("Error Occurs In Searching Schema: "+e.getMessage());}
			return found;
		}
		
}

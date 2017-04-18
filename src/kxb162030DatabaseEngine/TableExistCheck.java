package kxb162030DatabaseEngine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class TableExistCheck {
	static String FILEPATH="information_schema.schemata.tbl";
public static boolean checkForTableExistance(String table_name, String schema_name) 
{
	boolean output=true;
	try{
	RandomAccessFile binaryFile=new RandomAccessFile(FILEPATH,"rw");
	int byteReads=0;
	try {
		while(byteReads<binaryFile.length())
		{
			String SuggestedSchemaName="";
			byte SuggestedschemaLength=binaryFile.readByte();
			byteReads++;
			
			for(int i=0; i<SuggestedschemaLength;i++)
			{
				SuggestedSchemaName+=(char)binaryFile.readByte();
				byteReads++;	
			}
			
			if(SuggestedSchemaName.equals(schema_name))
					{
				String checkSuggestedName="";
				byte fieldLength=binaryFile.readByte();
				byteReads++;
				for(int i=0; i<fieldLength; i++)
				{
					checkSuggestedName+=(char)binaryFile.readByte();
					byteReads++;
				}
				if(checkSuggestedName.equals(table_name))
				{
					output=true;
					break;
				}
				else
				{
					byte nameLength1=binaryFile.readByte();
					byteReads++;
					for(int i=0; i<nameLength1; i++)
					{
						binaryFile.readByte();
						byteReads++;
					}
					//output=false;
				}
				
				
					}
		}
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	}catch(FileNotFoundException e)
	{
		System.out.println("File does not exist");
	}
	return output;
	
}
	
}

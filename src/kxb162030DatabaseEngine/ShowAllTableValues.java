package kxb162030DatabaseEngine;

import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class ShowAllTableValues {
	static final String FILEPATH = "information_schema.columns.tbl";
	
	public static void  ShowAllValues(String schemaName,String TableName){
		try{
		//RandomAccessFile TableFile = new RandomAccessFile(FILEPATH, "rw");
		RandomAccessFile schemataTableFile=new RandomAccessFile(schemaName+"."+TableName+".tbl", "rw");
		
		ArrayList<String> ColumnNamesInThisRow=RetrieveColumnInfo.getColumnNames(schemaName, TableName);
		//ArrayList<String> ColumnValuesInthisRow=RetrieveColumnInfo.
		for(int j=0; j<ColumnNamesInThisRow.size();j++)
		{
			System.out.print(ColumnNamesInThisRow.get(j)+"\t");
		}
		System.out.println();
		schemataTableFile.seek(0);
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
	/*	
		
		for(int k=0; k<ColumnNamesInThisRow.size();k++){
			String currentType=ColumnNamesInThisRow.get(k).toLowerCase().replace("\\s+", "");
			
			if(currentType.equals("byte"))
			{byte val=columnsTableFile.readByte();
			System.out.print(val+" |\t");}
			else if(currentType.equals("short")||currentType.equals("shortint"))
			{Short val=columnsTableFile.readShort();
			System.out.print(val+" |\t");}
			else if(currentType.equals("int"))
			{Integer val=columnsTableFile.readInt();
			System.out.print(val+" |\t");}
			else if(currentType.equals("long")||currentType.equals("longint"))
			{Long val=columnsTableFile.readLong();
			System.out.print(val+" |\t");}
			else if(currentType.equals("float"))
			{Float val=columnsTableFile.readFloat();
			System.out.print(val+" |\t");}
			else if(currentType.equals("double"))
			{Double val=columnsTableFile.readDouble();
			System.out.print(val+" |\t");}
			else if(currentType.equals("datetime"))
			{Long valinlong=columnsTableFile.readLong();
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
			Date date=new Date(valinlong);
			String dateinstring=formatter.format(date);
			System.out.print(dateinstring+" |\t");}
			else if(currentType.equals("date"))
			{Long valinlong=columnsTableFile.readLong();
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date date=new Date(valinlong);
			String dateinstring=formatter.format(date);
			System.out.print(dateinstring+" |\t");}
			else if(currentType.equals("char")||currentType.equals("varchar"))
			{
				String val="";
				byte length=columnsTableFile.readByte();
				for(int m=0; m<length; m++){
					val+=(char)columnsTableFile.readByte();
				}
				System.out.print(val+" |\t");
			}
			
			
			
		}
		System.out.println("");
		
		*/
		
		//int bytesRead=0;
		
		/*	
		while(bytesRead<columnsTableFile.length()){
		//System.out.println(columnsTableFile.length());
			ArrayList<String> TableValue=new ArrayList<>();
	
			String schemaNameString="";
			String tableNameString="";
			String columnNameString="";
			Integer positionInteger=0;
			String columnTypeString="";
			String Nulable="";
			String columnKey="";
			
			byte namelength=columnsTableFile.readByte();
			bytesRead++;
			System.out.println(namelength);
			for(int i=0 ; i<namelength; i++)
			{schemaNameString+=(char)columnsTableFile.readByte();
			bytesRead++;}
			System.out.print(schemaNameString+"\t");
			
			byte namelength2=columnsTableFile.readByte();
			bytesRead++;
			for(int i=0 ; i<namelength2; i++)
			{tableNameString+=(char)columnsTableFile.readByte();
			bytesRead++;}
			System.out.print(tableNameString+"\t");
			
			byte namelength3=columnsTableFile.readByte();
			bytesRead++;
			for(int i=0 ; i<namelength3; i++)
			{columnNameString+=(char)columnsTableFile.readByte();
			bytesRead++;}
			System.out.print(columnNameString+"\t");
			
			positionInteger=columnsTableFile.readInt();
			System.out.print(positionInteger+"\t");
			bytesRead+=4;
			
			byte namelength4=columnsTableFile.readByte();
			bytesRead++;
			for(int i=0 ; i<namelength4; i++)
			{columnTypeString+=(char)columnsTableFile.readByte();
			bytesRead++;}
			System.out.print(columnTypeString+"\t");
			
			byte namelength5=columnsTableFile.readByte();
			bytesRead++;
			for(int i=0 ; i<namelength5; i++)
			{Nulable+=(char)columnsTableFile.readByte();
			bytesRead++;}
			System.out.print(Nulable+"\t");
			
			byte namelength6=columnsTableFile.readByte();
			bytesRead++;
			for(int i=0 ; i<namelength6; i++)
			{columnKey+=(char)columnsTableFile.readByte();
			bytesRead++;}
			System.out.print(columnKey+"\t");
			System.out.println("Bytes Read: "+bytesRead);
		
		}
		*/
		System.out.println("Total size in bytes :"+schemataTableFile.length());
		}catch(Exception e){System.out.println(e.getMessage());}
	}

}

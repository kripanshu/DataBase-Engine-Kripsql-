package kxb162030DatabaseEngine;

import java.io.RandomAccessFile;
import java.util.ArrayList;

public class RetrieveColumnInfo {

	

	public static ArrayList<String> getColumnNames(String SchemaName, String TableName){

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
						dataTypes.add(InsertData.parsingType(columnTypeString));//parsing type name into list
						dataLength.add(InsertData.parsingTypeLength(columnTypeString));//parsing type length into list
						
						
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
		return columnNamesList;
	}
	
	public static ArrayList<String> getColumnTypes(String SchemaName, String TableName){

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
						dataTypes.add(InsertData.parsingType(columnTypeString));//parsing type name into list
						dataLength.add(InsertData.parsingTypeLength(columnTypeString));//parsing type length into list
						
						
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
		return dataTypes;
	}

	public static String getColumnType( String TargetColumnName, String SchemaName, String TableName){

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
						byte namelength3=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength3; i++)//read column name
						{columnNameString+=(char)columnsTableFile.readByte();
						bytesRead++;}
						//columnNamesList.add(columnNameString);//add column name into COLUMN_NAME list
						if(columnNameString.equals(TargetColumnName))
						{
						positionInteger=columnsTableFile.readInt();
						bytesRead+=4;//read cordination rank
			//!!!READ COLUMN TYPE (NAME+LENGTH)			
						byte namelength4=columnsTableFile.readByte();
						bytesRead++;
						for(int i=0 ; i<namelength4; i++)//read column type
						{columnTypeString+=(char)columnsTableFile.readByte();
						bytesRead++;}
						columnTypes.add(columnTypeString);//add raw column type (name+length) into type list
						String currentTypeName=InsertData.parsingType(columnTypeString);
						return currentTypeName;
						//dataTypes.add(InsertInto.parsingType(columnTypeString));//parsing type name into list
						//dataLength.add(InsertInto.parsingTypeLength(columnTypeString));//parsing type length into list
						
						}
						else
						{
							positionInteger=columnsTableFile.readInt();
							bytesRead+=4;//read cordination rank
				//!!!READ COLUMN TYPE (NAME+LENGTH)			
							byte namelength4=columnsTableFile.readByte();
							bytesRead++;
							for(int i=0 ; i<namelength4; i++)//read column type
							{columnTypeString+=(char)columnsTableFile.readByte();
							bytesRead++;}
							columnTypes.add(columnTypeString);//add raw column type (name+length) into type list
							String currentTypeName=InsertData.parsingType(columnTypeString);
							
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
				
			}
		}catch(Exception e){System.out.println("Error Occurs In Retrieve Constraints: "+e.getMessage());}
		return "";
	}

}

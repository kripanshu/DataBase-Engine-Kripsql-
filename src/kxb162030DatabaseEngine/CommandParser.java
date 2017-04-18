package kxb162030DatabaseEngine;

import java.util.ArrayList;

public class CommandParser {
	
	
	

		//Categorizing command types.
		public static int parseUserCommand(String userCommandparse){
			//String y="";
	String value=userCommandparse.toLowerCase();
	String [] commands={"show schemas","show tables","use ","create schema","create table ","insert into ","select *","show columns","help","drop table "}; 
			//String c="";
			
			
		
			if(value.startsWith(commands[0]))
				return 1;
			else if(value.startsWith(commands[1]))
				return 2;
			else if(value.startsWith(commands[2]))
				return 3;
			else if(value.startsWith(commands[3]))
				return 4;
			else if(value.startsWith(commands[4]))
				return 5;
			else if(value.startsWith(commands[5]))
				return 6;
			else if(value.startsWith(commands[6]))
				return 7;
			else if(value.startsWith(commands[7]))
				return 8;
			else if(value.startsWith(commands[8]))
				return 9;
			else if(value.startsWith(commands[9]))
				return 10;
			else
			return 0;
		}
		//EXTRACT schema name from USE command.
		public static String useCommandParser(String val){
			String [] schemaname=val.split("\\s+");
			String result=schemaname[1];
			return result;
		}
			//EXTRACT Schema_Name from CREATE SCHEMA command.
		public static String createSchemaParser(String val){
			String [] temp=val.split("\\s+");
			String result=temp[2];
			return result;
		}
			//EXTRACT Table_Name from CREATE TABLE command.
		public static String createTableNameParser(String val){
			String y=val.substring(0,val.indexOf('('));
			String [] temp=y.split("\\s+");
			String tableName=temp[2];
			return tableName;
		}
		
			//EXTRACT Column_Names from CREATE TABLE command.
		public static ArrayList<String> createTableColumnNamesParser(String val){
			String result=val.substring(val.indexOf('(')+1, val.length()-1);
			String [] eachResult = result.split(",");
		
			ArrayList<String> columnNames=new ArrayList<>();
			for(int i=0; i<eachResult.length; i++)
			{
				String [] data=eachResult[i].trim().split("\\s+");
				//String Name_and_Type = eachConstraint[i].substring(0, eachConstraint[i].indexOf('[')-1);
				String Name_and_Type=data[0];
				//System.out.println(Name_and_Type);
				
				columnNames.add(Name_and_Type);
			}
			return columnNames;
		}
		
		//EXTRACT Column_DataTypes from CREATE TABLE command.
		public static ArrayList<String> createTableDataTypeParser(String val){
			String result=val.substring(val.indexOf('(')+1, val.length()-1);
			String [] eachConstraint = result.split(",");
			
			ArrayList<String> columnType=new ArrayList<>();
			
			for(int i=0; i<eachConstraint.length; i++)
			{
				String Type="";
				String eachline=eachConstraint[i].toString().trim();
				String [] individualdata=eachline.split("\\s+");
				//System.out.println(individualdata[1]);
				Type=individualdata[1];
				columnType.add(Type);
			}
			return columnType;
		}
		
		//EXTRACT key constraint from CREATE TABLE command
		public static ArrayList<String> createTableKeyParser(String val){
			ArrayList<String> keys=new ArrayList<>();
			String result=val.substring(val.indexOf('(')+1, val.length()-1);
			String [] eachConstraint = result.split(",");
			
			for(int i=0; i<eachConstraint.length; i++)
			{//System.out.println(eachConstraint[i]);
				String keyStatus=eachConstraint[i].substring(eachConstraint[i].indexOf('[')+1, eachConstraint[i].indexOf('|')+1);
				String check= keyStatus.replaceAll("[^A-Za-z]+","").trim();
				//System.out.println(check.toLowerCase());
				if(check.toLowerCase().toString().equals("primarykey"))
				{
					keys.add(check);
				}else
				{keys.add("");}
				
				
			}
				
			return keys;
		}
		
		//EXTRACT Null constraint from CREATE TABLE command
		public static ArrayList<String> createTableNullValueParser(String val){
			ArrayList<String> Nulls=new ArrayList<>();
			String result=val.substring(val.indexOf('(')+1, val.length()-1);
			String [] eachConstraint = result.split(",");
			for(int i=0; i<eachConstraint.length; i++)
			{
				String NullStatus=eachConstraint[i].substring(eachConstraint[i].indexOf('[')+1, eachConstraint[i].indexOf(']')+1);
				if(NullStatus.equalsIgnoreCase(""))
				{
					Nulls.add("");}
				else {
					String newstr =NullStatus.replaceAll("[^A-Za-z]+", "").trim();
				Nulls.add(newstr);}
			}
			return Nulls;
		}
			//Extract Table Name Which To Insert Into.
		public static String parseInsertTableName(String x){
			String TableName="";
			String SubNoVal=x.substring(0,x.indexOf('('));
			String [] temp=SubNoVal.split("\\s+");
			TableName=temp[2];
			return TableName;
		}

		//Extract insert values
		public static ArrayList<String> parseInsertValues(String x){
			ArrayList<String> valueList=new ArrayList<>();
			String values=x.substring(x.indexOf('(')+1,x.indexOf(')'));
			
			String values1=values.replaceAll("\'", "");
			String [] temp=values1.split(",");
			for(int i=0; i<temp.length;i++)
			{valueList.add(temp[i].replaceAll("\\s+", ""));}
			return valueList;
		}
		
			//Extract Table Name for Select
		public static String parseSelectTableName(String x){
			String [] temp=x.split("\\s+");
			return temp[3];
		}
		
		//Extract column name
		public static String parseSelectColumnName(String x){
			String [] temp=x.split("\\s+");
			String columnName = temp[5].substring(0, parseSelectComparisonPosition1(temp[5]));
			return columnName;
		}
		
		//Extract compare
		public static int parseSelectComparison(String x){
			if(x.contains("="))
				return 1;
			else if(x.contains("<>"))
				return 2;
			else if(x.contains("<") && !x.contains(">"))
				return 3;
			else if(x.contains(">") && !x.contains("<"))
				return 4;
			else 
				return 0;
		}
		
		//Extract compare value
		public static String parseSelectValue(String x){
			String sub=x.substring(parseSelectComparisonPosition(x)+1,x.length());
			String sub1=sub.replaceAll("'","");
			String sub2=sub1.replaceAll("\\s+", "");
			return sub2;
		}
		
		//Extract Compare position
		public static int parseSelectComparisonPosition(String x){
			if(x.contains("="))
				return x.indexOf('=');
			else if(x.contains("<>"))
				return x.indexOf('>');
			else if(x.contains("<") && !x.contains(">"))
				return x.indexOf('<');
			else if(x.contains(">") && !x.contains("<"))
				return x.indexOf('>');
			else 
				return x.length();
		}
		
		public static int parseSelectComparisonPosition1(String x){
			if(x.contains("="))
				return x.indexOf('=');
			else if(x.contains("<>"))
				return x.indexOf('<');
			else if(x.contains("<") && !x.contains(">"))
				return x.indexOf('<');
			else if(x.contains(">") && !x.contains("<"))
				return x.indexOf('>');
			else 
				return x.length();
		}
		
	public static String parseDropTableName(String x){
		String [] temp=x.split("\\s+");
		return temp[2];
	}
		
	


}

package kxb162030DatabaseEngine;


import java.util.*;

public class KripanshuBaseSQL {
	
	
	static String prompt="kripsql>";
	static Boolean isExit= false ;
	static long pageSize = 512;
	static Scanner scanner=new Scanner(System.in).useDelimiter(";");
	static String currentSchemaName="null";
	
	public static void splashScreen()
	{
		//It prints the introduction part of the program
		System.out.println(line("-",70));
		System.out.println("Welcome to the KripSQL program.  Commands end with ;");
		System.out.println("Your KripSQL connection id is 4");
		System.out.println("version: 1.1");
		System.out.println("Copyright (c) 2017, Kripanshu and/or its affiliates. All rights reserved.");
		System.out.println("Author: Kripanshu Bhargava and NetID : kxb162030");
		System.out.println(line("-",70));
	}
	
	//In order to print the * line
	public static String line(String s, int num)
	{
		String val="";
		for(int i=0; i<num; i++)
		{
			val+=s;
			
		}
		
		return val;
		
	}
	
	
	/*
	 * The Main method 
	 */
	public static void main(String args[])
	{
		CreateInformationSchema.createInfotmationSchema();
		splashScreen();
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

		
		
		
	}

	private static void parseUserCommand(String userCommand) {
		
		//System.out.println("Check Kripanshu's program");
		ArrayList <String> commandTokens= new ArrayList <>(Arrays.asList(userCommand.split(" ")));
		switch(commandTokens.get(0))
		{
		case "select": parseSelectString(userCommand);
						break;
		case "select table" : ShowAllTableValues.ShowAllValues(currentSchemaName, "persons");
		break;
		case "insert" : parseInsertString(userCommand);
						break;
		case "drop"	: System.out.println("STUB: Calling your method to drop items");
						dropTable(userCommand);
						break;
		case "create" : parseCreateString(userCommand);
						break;
		case "help" : help();
						break;
		case "version" : getVersion();
						break;
		case "exit" : isExit=true;
						break;
		case "use" : getSchema(userCommand);
					break;
		case "show": getSchema(userCommand);
					break;
		case "quit" : isExit=true;
		
		}
		
		// TODO Auto-generated method stub
		
	}
	
	private static void getSchema(String userCommand) {
		// TODO Auto-generated method stub
		int c = CommandParser.parseUserCommand(userCommand);
		if(c==1)
		{
			ShowSchemas.showSchemas();
		}else if(c==3){
		currentSchemaName=CommandParser.useCommandParser(userCommand);
		Boolean val=UseSchema.useSchema(currentSchemaName);
		if(val)
		{
			System.out.println(currentSchemaName+" in use ");
		}else 
		{
			System.out.println(currentSchemaName + "does not exist");
		}
		
		}
		else if(c==2)
		{//currentSchemaName=UserCommanParser.parseUse(userCommand);
			ShowTables.showTables(currentSchemaName);
		}
		else if (c==8)
		{
			ShowColumns.ShowColumns();
		}
		
	}

	private static void getVersion() {
		// TODO Auto-generated method stub
		System.out.println("version: 1.1");
		
		
	}

	private static void parseCreateString(String userCommand) {
		
		// TODO Auto-generated method stub
		
		//System.out.println("#### kripson at work");
		int c=CommandParser.parseUserCommand(userCommand);
		if(c==4)
		{
		String SchemaName=CommandParser.createSchemaParser(userCommand);
		CreateScehma.CreateSchema(SchemaName);
		}
		else if(c==5)
		{
			if(currentSchemaName=="null")
			{
			System.out.println("Schema is not selected");	
			}
			else{
			String TableName=CommandParser.createTableNameParser(userCommand);
			ArrayList<String> columnNames=CommandParser.createTableColumnNamesParser(userCommand);
			ArrayList<String> columnTypes=CommandParser.createTableDataTypeParser(userCommand);
			boolean correctInput=InputValidation.TableCreationValidation(columnTypes);
			if(correctInput){
			ArrayList<String> columnNulable=CommandParser.createTableNullValueParser(userCommand);
			ArrayList<String> columnKey=CommandParser.createTableKeyParser(userCommand);
			CreateTable.createTable(currentSchemaName,TableName,columnNames,columnTypes,columnNulable,columnKey);
		
	}
		}
		}
		
	}

	private static void dropTable(String userCommand) {
		// TODO Auto-generated method stub
		if(currentSchemaName.equals(""))
		{System.out.println("Schema is not selected");
	}
		String TableNameForDrop=CommandParser.parseDropTableName(userCommand);
		boolean tableExistance2=CreateTable.checkForTableExistance(TableNameForDrop, currentSchemaName);
		if(!tableExistance2)
		{System.out.println("Table "+TableNameForDrop+"Does not Exist.");
		System.out.println("0 Rows affected");
		}
		
		DropTable.dropTable(currentSchemaName, TableNameForDrop);
		
	}

	private static void parseInsertString(String userCommand) {
		// TODO Auto-generated method stub
		String TableNameForInsert=CommandParser.parseInsertTableName(userCommand);
		ArrayList<String> InsertValues=CommandParser.parseInsertValues(userCommand);
		if(currentSchemaName.equals(""))
		{
			System.out.println("Schema is not selected");
		
		}
		//Table Exist?
		boolean tableExist=CreateTable.checkForTableExistance(TableNameForInsert, currentSchemaName);
		if(!tableExist)
		{
			System.out.println("Table "+TableNameForInsert+"Does not Exist.");
			System.out.println("0 Rows affected");
		}
		InsertData.InsertValues(currentSchemaName, TableNameForInsert, InsertValues);
		
	}

	private static void parseSelectString(String userCommand) {
		// TODO Auto-generated method stub
		
		//For ' select * from table_name ; '
		String [] check = userCommand.trim().split(" ");
	/*	for(int i=0; i<check.length;i++)
		{
			System.out.println(check[i]);
			
		}
		System.out.println(check.length);
		*/
		if(check.length==5 ||check.length==4 )
		{
			System.out.println("sucess");
			if(currentSchemaName.equals(""))
			{System.out.println("Schema is not selected");
			}
			String TableName= check[3];
			System.out.println(TableName);
			ShowAllTableValues.ShowAllValues(currentSchemaName, TableName);
		}
		// select * from table_name where rowid='' ;
		else{
		
		if(currentSchemaName.equals(""))
		{System.out.println("Schema is not selected");
		}
		String TableNameForSelection=CommandParser.parseSelectTableName(userCommand);
		boolean tableExistance=CreateTable.checkForTableExistance(TableNameForSelection, currentSchemaName);
		if(!tableExistance)
		{System.out.println("Table "+TableNameForSelection+"Doesn't Exist.");
		System.out.println("0 Rows affected");
		}
		String ColumnName=CommandParser.parseSelectColumnName(userCommand);
		String compValue=CommandParser.parseSelectValue(userCommand);
		Integer judgeSymble=CommandParser.parseSelectComparison(userCommand);
		ArrayList<Integer> locations=SelectData.getLocations(currentSchemaName, TableNameForSelection, ColumnName, compValue, judgeSymble);
		SelectData.printRow(currentSchemaName, TableNameForSelection, locations);
		}
	}

	/*for the help :
	 * Type 'show schemas;' to see all schama in database.
Type 'show tables;' to check tables of a schema in database
Type 'show columns;' to see all the columns information in database
Type 'use + schemaname;' to use schema you specified
Type 'create schema + schemaname' to build new schema in given name
Type 'create table + tablename +(columnname datatype [primary key|not null],```columnnameN datatype(datalength) [|])' to createtable
(Notice: key and null constraint must be specified in [primary key/(blank)|not null/(blank)]!)
Type insert into +tablename values(value1,value2,```valueN) to insert values in specified table
Type 'drop tablename;' to drop table
Type 'select * from +tablename+ where + columnname =/<>/</> + comapre value' to show rows in specified table
Type 'exit;' to leave DavisBase
*****************************************************************************";
		
	 */
	public static void help() {
		System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		
		System.out.println("\tuse + schemaname;							       to use schema you specified");
		System.out.println("\tshow columns;                                    to see all the columns information in database");
		System.out.println("\tshow tables;							           to check tables of a schema in database");
		System.out.println("\tcreate schema + schemaname      				   to build new schema in given name");
		System.out.println("\tcreate table + tablename +(columnname datatype [primary key|not null],```columnnameN datatype(datalength) [|]);  to createtable");
		System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
		System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
		System.out.println("\tVERSION;                                         Show the program version.");
		System.out.println("\tHELP;                                            Show this help information");
		System.out.println("\tEXIT;                                            Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}


}

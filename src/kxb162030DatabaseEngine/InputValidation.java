package kxb162030DatabaseEngine;

import java.util.ArrayList;

public class InputValidation {

	public static boolean TableCreationValidation(ArrayList<String> columnTypes) {
		// TODO Auto-generated method stub
		
boolean result=true;
		
		for(int i=0; i<columnTypes.size(); i++)
		{
			
			String CurrentTypeName=columnTypes.get(i).toLowerCase();
			
			if(!CurrentTypeName.contains("byte") && !CurrentTypeName.contains("int") && !CurrentTypeName.contains("short") && !CurrentTypeName.contains("short int") && !CurrentTypeName.contains("long") && !CurrentTypeName.contains("long int") && !CurrentTypeName.contains("char(") && !CurrentTypeName.contains("varchar(") && !CurrentTypeName.contains("float") && !CurrentTypeName.contains("double") && !CurrentTypeName.contains("datetime") && !CurrentTypeName.contains("date") )
			{	
				System.out.println(CurrentTypeName+"-----Data Type is Invalid! ");
				return false;
			}
			else if(CurrentTypeName.contains("char"))
			{
				if(!(CurrentTypeName.contains("(") && CurrentTypeName.contains(")")))
				{
					System.out.println(CurrentTypeName+"-----Data Type is Invalid!\nCHAR and VARCHAR Need to Specify length in format TYPE(length).\nFor Example: char(10).");
					return false;
				}
				if(CurrentTypeName.indexOf('(')+1==CurrentTypeName.indexOf(')'))
				{
					System.out.println(CurrentTypeName+"-----Data Type is Invalid! Please Enter The data Length. ");
					return false;
				}
			}
		}
		
		return result;
	
	}

}

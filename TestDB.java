//-----------------------------------------------------
// Example code to read from fixed length records (random access file)
//-----------------------------------------------------
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;



public class TestDB {
  static Record record;

  //display menu method is a user prompt that presents user with multiple options ranging from option 1 - option 9
  //scanner behaves according to input using switch case statements
  public static void display_menu() {
	  
	    System.out.println ( "\n***1) Create new database*** ***2) Open database*** ***3) Close database*** ***4) Display record*** \n***5) Update record*** ***6) Create report*** 7) Add record*** ***8) Delete record*** ***9) Quit***" );
	    
	  }
  
  public static void main(String[] args) throws IOException, InterruptedException {

	  
    // calls constructor
    DB db = new DB();
    
    int record_num = 0;
   
    String file = null;
    //scanner added to detect what the user types
    Scanner inputUser = new Scanner(System.in);
    
    
    //boolean condition to check if file is entered by user
    // give error if not
    
	boolean filePass = false;
	    
	//string variables with different extensions depending on file type
	
    String fileNameWithOutExt;
    String fileNameWithConfigExt;
    String fileNameWithDataExt = null ;
    String fileNameWithOverFlowExt;
    int reply = 0;
    display_menu();
    
    System.out.print("\nWhat operation would you like to perform (Type in the option number only): ");
    reply =  inputUser.nextInt();
    inputUser.nextLine();
    //switch case statements loop to see what option user enters and behaves accordingly
    while(reply != 9) {
    	 
		    
		   
		    String answerWarning = "";
		    switch ( reply ) {
		    
		      case 1:
		    	//CREATING THE IF CONDITION THAT IF ANOTHER DATABASE IS CREATED WHILE PREVIOUS DATABASE IS OPEN, GIVE THE OPTION TO CLOSE PREVIOUS DATABASE.
		    	  if(db.isOpen()) {
		    		  System.out.println("\nYou are about to create another database. Would you like to close previous database (Yes/No)");
		    		  String userReplyAnotherDB = inputUser.nextLine();
		    		  if(userReplyAnotherDB.equalsIgnoreCase("Yes")) {
		    			  db.close();
		    			  
		    		  }
		    	  }
		    	   filePass = false;
		    	
		    	   //filePass boolean to see if user has actually entered a correct file
		    	
		    	   while(filePass != true) {
		    			System.out.print("Please enter the name of the file you would like to open: ");
		    		    
		    		    file =  inputUser.nextLine();
		    		    File filecheck = new File(file); 
		    		  if(!filecheck.exists()) {
		    		 
		    			  System.out.println("\nCould not open file. File not found in directory...\n");
		    		  }else {
		    			  filePass=true;
		    		  }
		    		}
		    		
		    	   //formatting file types differently for config, data, and overflow
		    		//using replacefirst method to format variables
		    	
		    	   fileNameWithOutExt = file.replaceFirst("[.][^.]+$", "");
		    		fileNameWithConfigExt = fileNameWithOutExt+ ".config";
		    		fileNameWithDataExt = fileNameWithOutExt + ".data";
		    		fileNameWithOverFlowExt = fileNameWithOutExt+ ".overflow";
		    		
		    	    //Creating a file object to see if a file database .data exists and presenting
		    		// options based on user input. 
		    		//Giving user warning if file already exists and asking if needs to be replaced to crete new
		    	  File f = new File(fileNameWithDataExt); 
		    	  if(f.exists()) {
		    		  System.out.print("WARNING: THE DATABASE YOU ARE TRYING TO CREATE ALREADY EXISTS, CONFIRM TO CREATE A NEW INSTANCE OF THE SAME DATABASE*data loss warning* (Yes/No): ");
		    		  answerWarning = inputUser.nextLine();
		    		  
		    		  if(answerWarning.equalsIgnoreCase("Yes")) {
		    			  System.out.println("\nDatabase Config File Created: " + fileNameWithConfigExt);
				    	  System.out.println("Database Data File Created: " + fileNameWithDataExt);
				    	  System.out.println("Database Data File Created: " + fileNameWithOverFlowExt);
			    		  System.out.println ( "\n******************SUCCESSFUL OPERATION => DATABASE CREATED\nALERT ==> OPEN DATABASE TO ACCESS RECORDS******************" );
			    		  File fdelete = new File(fileNameWithDataExt);
			    		  File f1delete = new File(fileNameWithConfigExt);
			    		  File f2delete = new File(fileNameWithOverFlowExt);
				   //when creating a new database that already EXISTS, all existing .data files need to be erased 
			       //using three print writers to erase existing data to create new .data .config and .overflow
				    	  try (PrintWriter pw = new PrintWriter(fdelete)) {}
				          catch (IOException e) {
				              e.printStackTrace();
				          }
				    	  try (PrintWriter pw = new PrintWriter(f1delete)) {}
				          catch (IOException e) {
				              e.printStackTrace();
				          }
				    	  try (PrintWriter pw = new PrintWriter(f2delete)) {}
				          catch (IOException e) {
				              e.printStackTrace();
				          }
				    //.open creates new .data .config. and .overflow file
			    		  db.open(file);
				    	  
			    	//read and parse function from db.java	 
			    	  	  db.readAndparseCSV(file,fileNameWithDataExt);
			    	//write configuration gets access to private variable num records and writes it in .config file 
			    	//using buffered reader
			    	  	  db.writeConfiguration();
			    	  	db.setcurrentDBStatusOpen(false);
			    	  }else if(answerWarning.equalsIgnoreCase("No")) {
			    		  //db.setcurrentDBStatusOpen(true);
			    		  
			    	  }
		    	  }

		    	  else {
		    		  System.out.println ( "\n******************SUCCESSFUL OPERATION => DATABASE CREATED******************\nALERT ==> OPEN DATABASE TO ACCESS RECORDS" );
		    		  db.open(file);
		    		  db.readAndparseCSV(file,fileNameWithDataExt);
		    		  db.writeConfiguration();
		    	  }
		    	  
		    	 		    	   
			        break;
			  //case 2 is if the user wants the database opened
		      case 2:
		    	  boolean unknown = true;
		    	  while(unknown == true) {
			    	 try {
			    	  File f1 = new File(fileNameWithDataExt); 
			    	  if(f1.exists() && f1.isFile()) {
			    	    		
			    	    		db.open(file);
			    	    		//db.writeConfiguration();
			    	    		db.getConfiguration();
			    	    		
				    	    	System.out.println ( "\n******************SUCCESSFUL OPERATION => DATABASE OPENED******************" );
				    	    	db.setcurrentDBStatusOpen(true);
				    	    	
				    	    	
			    	    }else {
			    	    	System.out.println ( "\n******************ERROR: DATABASE NEEDS TO BE CREATED IN ORDER TO BE OPENED... :/******************" );
			    	    }
			    	  unknown = false;
			    	 }catch(NullPointerException e) {
			    		 filePass = false;
				    		while(filePass != true) {
				    			
				    			System.out.print("Please enter the name of the DB file (ONLY PREFIX) you would like to open: ");
				    		    
				    		    file =  inputUser.nextLine();
				    		    File filecheck = new File(file + ".data"); 
				    		  if(!filecheck.exists()) {
				    		 
				    			  System.out.println("\n******************Could not open file. File not found in directory... Create a new database or open another DB******************\n");
				    		  }else {
				    			  filePass=true;
				    		  }
				    		}
				    		fileNameWithOutExt = file.replaceFirst("[.][^.]+$", "");
				    		fileNameWithConfigExt = fileNameWithOutExt+ ".config";
				    		fileNameWithDataExt = fileNameWithOutExt + ".data";
				    		fileNameWithOverFlowExt = fileNameWithOutExt+ ".overflow";
				    		
			    	 }
		    	  }
		    	    
			    	break;
			  //case 3 is if the user wants to close the database
		      case 3:
			        db.close();
			        break;
			  //case 4 is if the user wants to display a specific record
			  //based on a certain id and binary search with find record
		      case 4:
		    	  if(db.isOpen()) {
			        System.out.print ( "Type in the ID for the record you would like to display: " );
			        int idInput = inputUser.nextInt();
			        String ID = String.valueOf(idInput);
			         record_num = db.findRecord(ID);
			        
			        if (record_num != -1) {
			          record = db.readRecord(record_num);
			          if(record_num < db.getNumRecords()) {
			          System.out
			              .println(
			                  "ID " + ID + " found at Record " + record_num + "\nRecordNum " + record_num + ": \n" + record.toString()
			                      );
			          }else if (record_num >= db.getNumRecords()) {
			        	  int overflowConvertedRecordNum = record_num - db.getNumRecords();
			        	  System.out
			              .println(
			                  "ID " + ID + " found at Record " + overflowConvertedRecordNum + " in the Overflow File\nRecordNum " + overflowConvertedRecordNum + "(Overflow File): \n" + record.toString()
			                      );
			          }
			          
			        } else
			          System.out.println("ID " + ID + " not found in our records");
		    	  }else {
		    		  System.out.println("\n******************ERROR: DATABASE IS NOT OPEN OR NEEDS TO BE CREATED... :/******************");
		    	  }
			        break;
			  //case 5 is if the user wants to update the record
		      case 5:
			          if(db.isOpen()) {
				        System.out.print ( "Enter the ID for the records you would like to update: " );
				        int idInput = inputUser.nextInt();
				        String ID = String.valueOf(idInput);
				         record_num = db.findRecord(ID);
				         if (record_num != -1) {
					          record = db.readRecord(record_num);
					          if(record_num < db.getNumRecords()) {
					          System.out
					              .println(
					                  "ID " + ID + " found at Record " + record_num + "\nRecordNum " + record_num + ": \n" + record.toString()
					                      );
					          }else if (record_num >= db.getNumRecords()) {
					        	  int overflowConvertedRecordNum = record_num - db.getNumRecords();
					        	  System.out
					              .println(
					                  "ID " + ID + " found at Record " + overflowConvertedRecordNum + " in the Overflow File\nRecordNum " + overflowConvertedRecordNum + "(Overflow File): \n" + record.toString()
					                      );
					          }
					          
					        
				          System.out.print ( "Enter the specific field index(Type '1' for State, '2' for City, '3' for Name) in ID " + idInput + " you would like to update:  " ); 
				          int fieldToChange = inputUser.nextInt();
				          inputUser.nextLine();
				          String fieldName = "";
				     //Asking user what field of the record they want altered
				          if(fieldToChange == 1) {
				        	  fieldName = "State";
				          }else if(fieldToChange == 2) {
				        	  fieldName = "City";
				          }else if(fieldToChange == 3) {
				        	  fieldName = "Name";
				          }
				          System.out.print ( "Enter the new field for " + fieldName + " you would like to update in ID " + idInput + ":"); 
				          String fieldNew = inputUser.nextLine();
				          record = db.updateRecord(ID, fieldToChange, fieldNew);
				          
				          //updated field shown
				         
					          System.out.print("\nFields for ID " + idInput + " are now updated to " + record.toString() + "\n\n");
				        } else
				          System.out.println("ID " + ID + " not found in our records");
			    	  }else {
			    		  System.out.println("\n******************ERROR: DATABASE IS NOT OPEN OR NEEDS TO BE CREATED... :/******************");
			    	  }
			        break;
			  //case 6 is if the user wants to create the ACSII report (10 lines of record)
		      case 6:
		    	  if(db.isOpen()) {
		    	  for(int i = 0; i<10; i++) {
			    	   int record_num1 = i;
	  	    	    record = new Record();
	  	    	    record = db.readRecord(record_num1);
	  	    	    if (!record.isEmpty())
	  	    	      System.out.println("RecordNum " + record_num1 + ": " + record.toString() + "\n\n");
	  	    	    else {
	  	    	      System.out.println("Could not get Record " + record_num1);
	  	    	      System.out.println("Record out of range");
	  	    	    }
		    	  } 
		    	  }else {
		    		  System.out.println("\n******************ERROR: DATABASE IS NOT OPEN OR NEEDS TO BE CREATED... :/******************");
		    	  }
			        break;
			  //case 7 is if the user wants to add a record 
		      case 7:
		    	  if(db.isOpen()) {

			        System.out.println ( "You chose to add a record. Enter the id to add: ");
			        String idAppended = inputUser.nextLine();
			        
			        System.out.println ( "Enter the state to add for id " + idAppended);
			        String stateAppended = inputUser.nextLine();
			        
			        System.out.println ( "Enter the city to add for id " + idAppended);
			        String cityAppended = inputUser.nextLine();
			        
			        System.out.println ( "Enter the name to add for id " + idAppended);
			        String nameAppended = inputUser.nextLine();
			        
			        db.addRecord(idAppended, stateAppended, cityAppended, nameAppended);
		    	  }else {
		    		  System.out.println("\n******************ERROR: DATABASE IS NOT OPEN OR NEEDS TO BE CREATED... :/******************");

		    	  }
			        break;
			  //case 8 is if the user wants to delete a particulr record
		      case 8:
		    	  if(db.isOpen()) {

			        System.out.println ( "Enter the id for the record you would like to delete: " );
			        String idToDelete = inputUser.nextLine();
			        db.deleteRecord(idToDelete);
			        System.out.println("Fields for id " + idToDelete + " are now deleted.");
		    	  }else {
		    		  System.out.println("\n******************ERROR: DATABASE IS NOT OPEN OR NEEDS TO BE CREATED... :/******************");

		    	  }
			        break;
			  //case 9 is if the user simply requests to exit the program
		      case 9:
		    	  
		    	 
		    	  if(db.isOpen())	
		    	  db.close();
		    	  	
			        System.exit(0);
			        break;
		      default:
		        System.err.println ( "Unrecognized option" );
		        break;
		    }
		    display_menu();
		    System.out.print("\nWhat operation would you like to perform (Type in the option number only): ");
		    
		    reply =  inputUser.nextInt();
		    inputUser.nextLine();
    	
    }
    
    //close database if open
    if(db.isOpen())
    db.close();
    System.out.println("Program Exited");
    

    
    
    
    //close Scanner
    inputUser.close();
    
    
   
    

    
  }
}

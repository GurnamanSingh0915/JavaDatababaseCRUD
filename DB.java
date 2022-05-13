import java.io.*;


public class DB {
 // public static final int NUM_RECORDS = num_records;
  public static final int RECORD_SIZE = 200;

  private RandomAccessFile dataFile;
  private File configFile;
  private RandomAccessFile overflowFile;
  private int num_records;
  private int overflow_records;
  private boolean currentDBStatusOpen=false;
   String[] parsedFields;
  String fileNameWithOutExt;
  String fileNameWithConfigExt;
  String fileNameWithDataExt;
  String fileNameWithOverFlowExt;

  public DB() {
    this.dataFile = null;
    this.overflowFile = null;
    this.configFile = null;
    this.num_records = 0;
    this.overflow_records = 0;
  }

  public void setNumRecordsBackToZero() {
	  this.num_records = 0;
	  this.overflow_records	= 0;
  }
  /**
   * Opens the file in read/write mode
   * 
   * @param filename (e.g., input.txt)
   * @return status true if operation successful
 * @throws IOException 
   */
  public void open(String filename) throws IOException {
    // Set the number of records
    

    // Open file in read/write mode
    //open and creates the database 
	  //randomaccessfile checks if a file exists
	  //if a file does not exists
	  //it automatically creates a new file.
    try {
  	    this.fileNameWithOutExt = filename.replaceFirst("[.][^.]+$", "");
        this.fileNameWithConfigExt = fileNameWithOutExt+ ".config";
        this.fileNameWithDataExt = fileNameWithOutExt + ".data";
        this.fileNameWithOverFlowExt = fileNameWithOutExt+ ".overflow";
        
      this.configFile = new File(fileNameWithConfigExt);
    
     
      this.dataFile = new RandomAccessFile(fileNameWithDataExt, "rw");
      
      this.overflowFile = new RandomAccessFile(fileNameWithOverFlowExt, "rw");
      
      
         
      currentDBStatusOpen = true;
      
      
  	  }catch(FileNotFoundException e) {
  	      System.out.println("\nCould not open file\n");
  	      e.printStackTrace();
  	     
  	      currentDBStatusOpen = false;
  	    }	
        

    
  }
  //write configuration that receives incremented number records and sets the values accordingly
  //size of fields and num records in the config file
  public void writeConfiguration() throws IOException {
	  BufferedWriter writerConfig = new BufferedWriter(new FileWriter(this.configFile));
      writerConfig.write("Number of records: " + this.num_records + "\n" + "Size of the fields: " +  RECORD_SIZE + "\n" + "Number of overflow records: " +  overflow_records + "\n");
      writerConfig.write("id,state,city,name\n");
      writerConfig.close();
  }
  //read configuration file to see the saved num records 
  public void getConfiguration() throws IOException {
	  BufferedReader br = new BufferedReader(new FileReader(this.configFile));		

	     
	  String contentLine = br.readLine();
	  String[] arrOfStr = contentLine.split("\\s+", 0);
  
  		this.num_records = Integer.parseInt(arrOfStr[arrOfStr.length-1]);  
  		br.readLine();
  		String contentLastLine = br.readLine();
  		String[] arrOfStrLast = contentLastLine.split("\\s+", 0);
  		this.overflow_records = Integer.parseInt(arrOfStrLast[arrOfStrLast.length-1]);  
  		
  		String fieldsToAdd = br.readLine(); 
  		this.parsedFields = fieldsToAdd.split(",",0);
  		
  		br.close();
  }
  public String[] getFields() {
	  return this.parsedFields;
  }
  public void setFields(String[] fields) {
	  this.parsedFields = fields;
  }
  //setcurrentDBStatusOpen status that reports whether or not the .data database is open
  
   public void setcurrentDBStatusOpen(boolean status) {
	  currentDBStatusOpen = status;
	  }
   public int getNumRecords() {
		  return num_records;
		  }

  /**
   * Close the database file
   */
  public void close() {
    try {
      dataFile.close();
      
      overflowFile.close();
      
      System.out.println("\nDatabase Closed");
      num_records = 0;
      overflow_records = 0;

      currentDBStatusOpen = false;
    } catch (IOException e) {
      System.out.println("There was an error while attempting to close the database file.\n");
      e.printStackTrace();
    }
  }
  
//  check to see if file status is "open"
  public Boolean isOpen() {
	  if(currentDBStatusOpen) {
		  
		  return true;
	  }else {
		  return false;
	  }
  }
 //this function empties out a string by trim method to see if it is empty.
  public static boolean isStringAllWhiteSpace(String str)
  {

      if (str.trim().isEmpty())
          return true;
      else
          return false;
  }
  /**
   * Get record number n (Records numbered from 0 to NUM_RECORDS-1)
   * 
   * @param record_num
   * @return values of the fields with the name of the field and
   *         the values read from the record
 * @throws IOException 
   */
  public Record readRecord(int record_num) throws IOException{
   
	Record record = new Record();
    String[] fields = new String[4];
    if(isOpen()) {
    	
    if ((record_num >= 0) && (record_num < this.num_records)) {
      try {
        dataFile.seek(0); // return to the top of the file
        dataFile.skipBytes(record_num * RECORD_SIZE);
        // parse record and update fields
        String[] fieldsWithSpacing = dataFile.readLine().split(",", 0);
        for(int i = 0; i<fieldsWithSpacing.length;i++) {
        	if(isStringAllWhiteSpace(fieldsWithSpacing[i])) {
        		fields[i] = null;
        	}else {
        		fields[i] = fieldsWithSpacing[i].trim();
        	}
        	
        }
        //fields = dataFile.readLine().split("\\s+", 0);
        
        record.updateFields(fields);
      } catch (IOException e ) {
        System.out.println("\n---THERE WAS AN ERROR WHILE ATTEMPTING TO READ A RECORD FROM THE DATABASE FILE OR THE RECORD HAS BEEN DELETED (Record Possibly Never Existed).---\n");
      }
     
    }else if((record_num >= 0) && (record_num >= this.num_records) && ((record_num - this.num_records) < this.overflow_records)) {
    	 try {
    	        overflowFile.seek(0); // return to the top of the file
    	        overflowFile.skipBytes((record_num - this.num_records) * RECORD_SIZE);
    	        // parse record and update fields
    	        
    	        String[] fieldsWithSpacing = overflowFile.readLine().split(",", 0);
    	        for(int i = 0; i<fieldsWithSpacing.length;i++) {
    	        	if(isStringAllWhiteSpace(fieldsWithSpacing[i])) {
    	        		fields[i] = null;
    	        	}else {
    	        		fields[i] = fieldsWithSpacing[i].trim();
    	        	}
    	        	
    	        }
    	    	        record.updateFields(fields);
    	      } catch (IOException e ) {
    	        System.out.println("\n---THERE WAS AN ERROR WHILE ATTEMPTING TO READ A RECORD FROM THE DATABASE FILE OR THE RECORD HAS BEEN DELETED.---\n");
    	      }
    }
    
   
    }
    return record;
  }
  	//writeRecord is a boolean return method that uses writeBytes method to get readandParse fields input and write 
  // data in the .data database using UTF-8 encoding
  public Boolean writeRecord(RandomAccessFile file, String id, String state, String city, String name) throws IOException  {

	  
	  int length = 199;
	  if(isOpen()) {
		  if(file == this.dataFile) {
		  
		  String formattedCodeOne = state;
	      String formattedCodeTwo = city;
	      String formattedCodeThree = name;
	      for(int i = state.length();i<30;i++) {
	    	  formattedCodeOne += " ";
	      }
	      for(int x = city.length();x<30;x++) {
	    	  formattedCodeTwo += " ";
	      }
	      
	      String finalRecord = id + "  ,  " + formattedCodeOne + "  ,  " + formattedCodeTwo + "  ,  " + formattedCodeThree;
	      
		  
	      file.writeBytes(finalRecord);
		  for(int i = finalRecord.length(); i<length;i++) {
			  file.writeBytes(" ");
		  }
		  file.writeBytes("\n");
		  this.num_records++;
		  }else if (file == overflowFile) {
			
				  String formattedCodeOne = state;
			      String formattedCodeTwo = city;
			      String formattedCodeThree = name;
			      for(int i = state.length();i<30;i++) {
			    	  formattedCodeOne += " ";
			      }
			      for(int x = city.length();x<30;x++) {
			    	  formattedCodeTwo += " ";
			      }
			      
			      String finalRecord = id + "  ,  " + formattedCodeOne + "  ,  " + formattedCodeTwo + "  ,  " + formattedCodeThree;
			      
				  
			      file.writeBytes(finalRecord);
				  for(int i = finalRecord.length(); i<length;i++) {
					  file.writeBytes(" ");
				  }
				  file.writeBytes("\n");
				  
				  
		  
		  
		  }
		  return true;
	  }else {
		  System.out.println("The database is not open...Therefore, could not edit any record. ");
		  return false;
	  }
    }
  
  //gets user entered id from TestDB scanner and uses binary search to find the record in the .data database
  //binarysearch happens after the condition that database is open, NOT CLOSED
  public int findRecord( String id) throws IOException  {
	  int recordNum = 0;
	  boolean throwError = false;
	  if(isOpen()) {
	    recordNum = binarySearch(id);
	    if(recordNum == -1) {
	    	boolean notFound = true;
	    	int overflowRecordNum = -1;
	    	while(notFound) {
	    		try {
	    	overflowRecordNum++;
	    	overflowFile.seek(0); // return to the top of the file
	    	
	    	overflowFile.skipBytes(overflowRecordNum * RECORD_SIZE);
	        // parse record and update fields
	        
	        
			//String[] fields = overflowFile.readLine().split("\\s+", 0);
			//String[] fields = new String[4];
	    	String[] fields = overflowFile.readLine().split("\\s+,\\s+", 0);
//	        for(int i = 0; i<fieldsWithSpacing.length;i++) {
//	        	if(isStringAllWhiteSpace(fieldsWithSpacing[i])) {
//	        		fields[i] = "";
//	        	}else {
//	        		fields[i] = fieldsWithSpacing[i].trim();
//	        	}
//	        	
//	        }
	        
			if(fields[0].equalsIgnoreCase(id)) {
				notFound = false;
			}
	    		}  catch (IOException e ) {
	    	        System.out.println("\n---THERE WAS AN ERROR WHILE ATTEMPTING TO READ A RECORD FROM THE DATABASE FILE OR THE RECORD HAS BEEN DELETED (Record Possibly Never Existed).---\n");
	    	        notFound =false;
	    	         throwError = true;
	    	        recordNum = -1;
	    		}catch(NullPointerException e) {
	    	    	  System.out.println("\n---THERE WAS AN ERROR WHILE ATTEMPTING TO READ A RECORD FROM THE DATABASE FILE OR THE RECORD HAS BEEN DELETED (Record Possibly Never Existed).---\n");
	    	    	    notFound=false;
	    	    	    throwError = true;
	    	    	   recordNum = -1;
	    		}
			
	    	}
	    	if(throwError == true) {
	    		recordNum = -1;
	    	}else {
	    	recordNum = overflowRecordNum + num_records;
	    	}
	    }
	  }else {
		  System.out.println("ERROR: DATABASE IS NOT OPEN OR NEEDS TO BE CREATED... :/");
	  }
	  
	return recordNum;
    }
  
  //updateRecord finds the record using the id and binary search and overwrites record using the overwriteRecord method by 
  //sending new fields to alter and the id to alter
  public Record updateRecord( String id, int fieldToChange, String fieldNew) throws IOException  {
	  	  Record record = new Record();
	  	  int record_num = findRecord(id);
		
		    if(isOpen()) {

		    	record = overwriteRecord(record_num,fieldToChange, fieldNew);
		   
		    
		  		 
	  }else {
		  System.out.println("The database is not open...Therefore, could not edit any record. ");
		  
	  }
	  return record;
	  
    }
  //overwriteRecord receives data to change from the updateRecord
  //needs parameters of the field to change record number and the new field itself.
  public Record overwriteRecord( int record_num, int fieldToChange, String fieldNew) throws IOException  {
  	  int length = 199;
  	  Record record = new Record();
  	  String[] fields = new String[4];
  	 if(isOpen()) {
	  if ((record_num >= 0) && (record_num < this.num_records)) {
	      try {
	        dataFile.seek(0); // return to the top of the file
	        dataFile.skipBytes((record_num) * RECORD_SIZE);
	        
	        
	        
	        // parse record and update fields
	        String[] fieldsWithSpacing = dataFile.readLine().split(",", 0);
	        for(int i = 0; i<fieldsWithSpacing.length;i++) {
	        	if(isStringAllWhiteSpace(fieldsWithSpacing[i])) {
	        		fields[i] = "";
	        	}else {
	        		fields[i] = fieldsWithSpacing[i].trim();
	        	}
	        	
	        }
	            
	       // fields = dataFile.readLine().split("\\s+", 0);
	        fields[fieldToChange] = fieldNew;
	        dataFile.seek(0); // return to the top of the file
	        //Go back to beginning to rewrite
	        dataFile.skipBytes((record_num) * RECORD_SIZE);
	        for(int i1 = 0; i1<length;i1++) {
				  dataFile.write("".getBytes("UTF-8"));
			  }
	        
	        writeRecord(dataFile, fields[0], fields[1], fields[2], fields[3]);
	        //updated Fields
	        
	        
	        record.updateFields(fields);
	        
	      } catch (IOException e ) {
	        System.out.println("There was an error while attempting to read a record from the database file.\n");
	        e.printStackTrace();
	      }
	      catch (NullPointerException e) {
	               
	      }
	    }else if((record_num >= 0) && (record_num >= this.num_records) && ((record_num - this.num_records) < this.overflow_records)) {
	      	
	  		try {
		        overflowFile.seek(0); // return to the top of the file
		        overflowFile.skipBytes((record_num - this.num_records) * RECORD_SIZE);
		        
		        
		        
		        // parse record and update fields
		        String[] fieldsWithSpacing = overflowFile.readLine().split(",", 0);
		        for(int i = 0; i<fieldsWithSpacing.length;i++) {
		        	if(isStringAllWhiteSpace(fieldsWithSpacing[i])) {
		        		fields[i] = "";
		        	}else {
		        		fields[i] = fieldsWithSpacing[i].trim();
		        	}
		        	
		        }
		        
		        fields[fieldToChange] = fieldNew;
		        overflowFile.seek(0); // return to the top of the file
		        //Go back to beginning to rewrite
		        overflowFile.skipBytes((record_num - this.num_records) * RECORD_SIZE);
		        for(int i = 0; i<length;i++) {
		        	overflowFile.write("".getBytes("UTF-8"));
				  }
		        
		        writeRecord(overflowFile, fields[0], fields[1], fields[2], fields[3]);
		        //updated Fields
		        
		        setFields(fields);
    	        record.updateFields(fields);
	        
	      }catch (IOException e ) {
		        System.out.println("There was an error while attempting to read a record from the database file.\n");
		        e.printStackTrace();
		      }
		      catch (NullPointerException e) {
		               
		      }
	  	
	  	}	    
	  		 
  }else {
	  System.out.println("The database is not open...Therefore, could not edit any record. ");
	  
  }
  	return record;
  
}
  
  //method overriding the overwriteRecord
  //if the parameters are more specific, or there are more fields to change
  public Record overwriteRecord( int record_num, int fieldOne, int fieldTwo, int fieldThree, String fieldOneNew, String fieldTwoNew, String fieldThreeNew) throws IOException  {
  	  int length = 199;
  	  Record record = new Record();
  	  String[] fields = new String[4];
  	 if(isOpen()) {
	  if ((record_num >= 0) && (record_num < this.num_records)) {
	      try {
	        dataFile.seek(0); // return to the top of the file
	        dataFile.skipBytes((record_num) * RECORD_SIZE);
	        
	        
	        
	        // parse record and update fields
	        String[] fieldsWithSpacing = dataFile.readLine().split(",", 0);
	        for(int i = 0; i<fieldsWithSpacing.length;i++) {
	        	if(isStringAllWhiteSpace(fieldsWithSpacing[i])) {
	        		fields[i] = "";
	        	}else {
	        		fields[i] = fieldsWithSpacing[i].trim();
	        	}
	        	
	        }
	       
	        //fields = dataFile.readLine().split("\\s+", 0);
	        fields[fieldOne] = fieldOneNew;
	        fields[fieldTwo] = fieldTwoNew;
	        fields[fieldThree] = fieldThreeNew;
	        dataFile.seek(0); // return to the top of the file
	        //Go back to beginning to rewrite
	        dataFile.skipBytes((record_num) * RECORD_SIZE);
	        for(int i = 0; i<length;i++) {
				  dataFile.write("".getBytes("UTF-8"));
			  }
	        
	        writeRecord(dataFile, fields[0], fields[1], fields[2], fields[3]);
	        //updated Fields
	        
	        
	        record.updateFields(fields);
	        
	      } catch (IOException e ) {
	        System.out.println("There was an error while attempting to read a record from the database file.\n");
	        e.printStackTrace();
	      }
	      catch (NullPointerException e) {
	               
	      }
	    }else if((record_num >= 0) && (record_num >= this.num_records) && ((record_num - this.num_records) < this.overflow_records)) {
	    	try {
		        overflowFile.seek(0); // return to the top of the file
		        overflowFile.skipBytes((record_num - this.num_records) * RECORD_SIZE);
		        
		        
		        
		        // parse record and update fields
		        String[] fieldsWithSpacing = overflowFile.readLine().split(",", 0);
		        for(int i = 0; i<fieldsWithSpacing.length;i++) {
		        	if(isStringAllWhiteSpace(fieldsWithSpacing[i])) {
		        		fields[i] = "";
		        	}else {
		        		fields[i] = fieldsWithSpacing[i].trim();
		        	}
		        	
		        }
		        // fields = overflowFile.readLine().split("\\s+", 0);
		        fields[fieldOne] = fieldOneNew;
		        fields[fieldTwo] = fieldTwoNew;
		        fields[fieldThree] = fieldThreeNew;
		        overflowFile.seek(0); // return to the top of the file
		        //Go back to beginning to rewrite
		        overflowFile.skipBytes((record_num - this.num_records) * RECORD_SIZE);
		        for(int i = 0; i<length;i++) {
		        	overflowFile.write("".getBytes("UTF-8"));
				}
		        
		        writeRecord(overflowFile, fields[0], fields[1], fields[2], fields[3]);
		        //updated Fields
		        
		        
    	        record.updateFields(fields);
		      } catch (IOException e ) {
		        System.out.println("There was an error while attempting to read a record from the database file.\n");
		        e.printStackTrace();
		      }
		      catch (NullPointerException e) {
		               
		      }
	    }
	   
	    
	  		 
  }else {
	  System.out.println("The database is not open...Therefore, could not edit any record. ");
	  
  }
  	return record;
  
}
  	public void deleteRecord(String idToDelete) throws IOException {
  		int num_record = findRecord(idToDelete);
  		Record getFields = readRecord(num_record);
  		String[] fields = {getFields.Id, getFields.State, getFields.City, getFields.Name};
  		
  		String fieldOneGone = ""; 
  		String fieldTwoGone = ""; 
  		String fieldThreeGone = ""; 
  		for(int i = 0; i<fields[1].length(); i++) {
  			fieldOneGone += " ";
  		}
  		for(int i = 0; i<fields[2].length(); i++) {
  			fieldTwoGone += " ";
  		}
  		for(int i = 0; i<fields[3].length(); i++) {
  			fieldThreeGone += " ";
  		}
  		overwriteRecord(num_record, 1,2,3, fieldOneGone, fieldTwoGone,fieldThreeGone);
  	
  		
  	}
  	
  	public void appendRecord(String idAppended, String stateAppended, String cityAppended, String nameAppended) throws IOException {
  		
  	  int length = 199;
	  if(isOpen()) {
		  String formattedCodeOne = stateAppended;
	      String formattedCodeTwo = cityAppended;
	      String formattedCodeThree = nameAppended;
	      for(int i = stateAppended.length();i<30;i++) {
	    	  formattedCodeOne += " ";
	      }
	      for(int x = cityAppended.length();x<30;x++) {
	    	  formattedCodeTwo += " ";
	      }
	      
	      String finalRecord = idAppended + "  ,  " + formattedCodeOne + "  ,  " + formattedCodeTwo + "  ,  " + formattedCodeThree;
	      
		  byte[] bytes = finalRecord.getBytes("UTF-8");
		  overflowFile.skipBytes( (int)overflowFile.length() );
		  overflowFile.write(bytes);
		  for(int i = finalRecord.length(); i<length;i++) {
			  overflowFile.write(" ".getBytes("UTF-8"));
		  }
		  overflowFile.write("\n".getBytes("UTF-8"));
		  this.overflow_records++;
		  
		  File fdelete = new File(this.fileNameWithConfigExt);
		  
   //when creating a new database that already EXISTS, all existing .data files need to be erased 
   //using three print writers to erase existing data to create new .data .config and .overflow
    	  try (PrintWriter pw = new PrintWriter(fdelete)) {}
          catch (IOException e) {
              e.printStackTrace();
          }
    	  writeConfiguration();
		  
	  }else {
		  System.out.println("The database is not open...Therefore, could not edit any record. ");
	  }
  	}
  	
  	public void addRecord(String idAppended, String stateAppended, String cityAppended, String nameAppended) throws IOException {
  	
  		appendRecord(idAppended, stateAppended, cityAppended, nameAppended);
  	}
  
  	//parsing csv input file 
  	//method uses Buffered library to read and write in the new .data file
  	//calls writeRecord method to take fields from csv and writeBytes in the .data file
  	public void readAndparseCSV(String file, String dataFile) throws IOException {
  		setNumRecordsBackToZero();
  		String indicatedfile = file,txtFile=dataFile;
  	    BufferedReader br = null;//For Read CSV File
  	    BufferedWriter br2=null;//For Write a file in which you want to write
  	    String words = "";
  	    
  	    //try catch statement to see if appropriate file is imported
  	    try {

  	    //calling Buffered functions from buffered file library
  	    //imported at top
  	     br = new BufferedReader(new FileReader(indicatedfile));
  	     br2=new BufferedWriter(new FileWriter(txtFile));
  	    //reading each and every line of csv file until null
  	     while ((words = br.readLine()) != null) {
  	   
  	      String[] code = new String[4];
  	      int currentStringIndex = 0;
  	      String currentWordToAdd = "";
  	      for(int current = 0; current < words.length(); current++) {
  	    //adding words based on delimeters
  	       if((words.charAt(current) == ',' && words.charAt(current+1) != '_')){
  	    	
  	    		code[currentStringIndex] = currentWordToAdd;
  	    		currentWordToAdd = "";
  	    		currentStringIndex++;
  	    	}else if(current == words.length()-1) {
  	    		currentWordToAdd += String.valueOf(words.charAt(current));
  	    		code[currentStringIndex] = currentWordToAdd;
  	    		currentWordToAdd = "";
  	    		currentStringIndex++;
  	    	}
  	    	else {
  	    		currentWordToAdd += String.valueOf(words.charAt(current));	
  	    	}
  	    	
  	      }
//calling write Record function to pass state, city, name field
  	      writeRecord(this.dataFile, code[0], code[1], code[2], code[3]);
  	     }
  	     

  	    } catch (FileNotFoundException e) {
  	     e.printStackTrace();
  	    } catch (IOException e) {
  	     e.printStackTrace();
  	    } finally {
  	     if (br != null) {
  	      try {
  	       br.close();
  	      } catch (IOException e) {
  	       e.printStackTrace();
  	      }
  	     }
  	     br.close();
  	     br2.close();
  	    }

  	}
  	

  /**
   * Binary Search by record id
   * 
   * @param id
   * @return Record number (which can then be used by read to
   *         get the fields) or -1 if id not found
 * @throws IOException 
   */
  public int binarySearch(String id) throws IOException {
  // System.out.println(this.num_records + " this is num records");
	int Low = 0;
    int High = this.num_records - 1;
    int Middle = 0;
    boolean Found = false;
    Record record;
    //System.out.println("Read Record 214616 " + readRecord(1773).Id );
    while (!Found && (High >= Low)) {
      Middle = (Low + High) / 2;
      record = readRecord(Middle);
      String MiddleId = record.Id;

      // int result = MiddleId[0].compareTo(id); // DOES STRING COMPARE
      int result = Integer.parseInt(MiddleId) - Integer.parseInt(id); // DOES INT COMPARE of MiddleId[0] and id
      if (result == 0)
        Found = true;
      else if (result < 0)
        Low = Middle + 1;
      else
        High = Middle - 1;
    }
    if (Found) {
      return Middle; // the record number of the record
    } else
      return -1;
  }
}

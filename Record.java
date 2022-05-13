import java.io.IOException;

public class Record {

  private boolean empty;

  public String Id;
  public String State;
  public String City;
  public String Name;


  public Record() {
    empty = true;
  }
  
  /**
   * Update the fields of a record from an array of fields
   * 
   * @param fields array with values of fields
   * @return nothing
   * @throws IOException
   */
  public void updateFields(String[] fields) throws IOException {
	 DB db = new DB();
	String[] fieldsNew = db.getFields();
    if (fields.length == 4) {
      this.Id = fields[0];
      this.State = fields[1];
      this.City = fields[2];
      this.Name = fields[3];
      

      empty = false;
    } else
      throw new IOException();
  }

  /**
   * Check if record fields have been updated
   * 
   * @return true if record has been updated otherwise false
   */
  public boolean isEmpty() {
    return empty;
  }

  //return statement that is neatly formatted
  public String toString() {
    return "Id: " + this.Id +
        ", State: " + this.State +
        ", City: " + this.City +
        ", Name: " + this.Name ;
  }

}

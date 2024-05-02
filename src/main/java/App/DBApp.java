package App;

/** * @author Wael Abouelsaadat */

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Arrays;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import BTree.BTree;
import Exceptions.DBAppException;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;

public class DBApp {

	private Vector<String> myTables;

	public DBApp() {

		myTables = new Vector<>();
		File folder = new File("TableFolders");
		File[] files = folder.listFiles();

		// Add all table folder names to the myTables vector.
		if (!(files == null)) {
			for (File f : files) {
				myTables.add(f.getName());
			}
		}

	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {

	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

		// If invalidated, exception is thrown.
		validateTableCreation(strTableName, strClusteringKeyColumn, htblColNameType);

		Table newTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType);

		// add table name to the table names vector
		myTables.add(strTableName);

		// write initial metadata, index assumed to be null at start
		writeMetadataAtStart(newTable);

		// creates the table folder
		File createFolder = new File("TableFolders//" + newTable.getTableName());
		createFolder.mkdir();

		// serialize the table object
		Serializer.serializeTable(newTable);
	}

	// Method to write initial metadata using CSVWriter Object
	private void writeMetadataAtStart(Table newTable) throws IOException {
		CSVWriter writer = new CSVWriter(new FileWriter("metadata//metadata.csv", true));

		Enumeration<String> columnNames = newTable.getColumnNameType().keys();
		Boolean isClustering;

		while (columnNames.hasMoreElements()) {
			String columnName = columnNames.nextElement();
			String columnType = newTable.getColumnNameType().get(columnName);

			isClustering = (newTable.getClusteringKey().equals(columnName)) ? true : false;

			String[] row = { newTable.getTableName(), columnName, columnType, String.valueOf(isClustering), "null",
					"null" };
			writer.writeNext(row);
		}

		writer.flush();
		writer.close();

	}

	// Method to validate the table's creation.
	private void validateTableCreation(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		// check if table was previously created.
		for (String s : myTables) {
			if (s.equals(strTableName)) {
				throw new DBAppException("Table already exists.");
			}
		}

		// check if the clustering key element exists in the hashtable.
		if (!(htblColNameType.containsKey(strClusteringKeyColumn))) {
			throw new DBAppException("Key doesn't exist in table.");
		}

		// check if data types are valid
		for (String dataType : htblColNameType.values()) {

			if (!(dataType.equals("java.lang.Integer")
					|| dataType.equals("java.lang.String")
					|| dataType.equals("java.lang.Double"))) {

				throw new DBAppException("Invalid Data Types Used.");
			}
		}
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException, ClassNotFoundException, IOException {

		//Make sure table exists first : 
		if(!tableExists(strTableName)) {
			throw new DBAppException("Table doesn't exist.");
		}
		
		Table table = getTable(strTableName);
		
		validateIndexCreation(table, strColName);
		
		BTree tree = new BTree();
		
	
		
		int numOfPages = table.getTablePages().size();
		
		// Create index for all existing tuples.
		for(int i = 0; i < numOfPages; i++){
			Page page = table.getPage(i);
			Vector<Tuple> tuples = page.getpageTuples();
			
			for(Tuple tuple : tuples) {
				Comparable columnValue = (Comparable) tuple.getValueOfColumn(strColName);
				
				TupleReference tupRef = new TupleReference(tuple.getClusteringKey(), tuple.getPageNum());
				
				tree.insert(columnValue, tupRef);
				
			}
		}
		
		tree.commit();
		table.getIndices().put(strColName, tree);
		updateMetaData(strTableName, strIndexName, strColName);
		
		Serializer.serializeTable(table);
	}
	
	

	private void updateMetaData(String tableName, String indexName, String colName) throws IOException {
		CSVReader reader = new CSVReader(new FileReader("metadata//metadata.csv"));
		List<String[]> allTablesCol = reader.readAll();
		List<String[]> filteredData = new ArrayList<>();
		reader.close();
				
		int size = allTablesCol.size();
		
		for(String[] row : allTablesCol) {
			
			//Add all other tables
			if(!(row[0].equals(tableName))) {
				filteredData.add(row);
			}
			
			//Add all other columns
			if(!(row[1].equals(colName))) {
				filteredData.add(row);
			}else {
				//Modify specified column within table.
				if(row[0].equals(tableName) && row[1].equals(colName)) {
					row[4] = indexName;
					row[5] = "B+tree";
					
					filteredData.add(row);
				}
				
			}
		}
		
		CSVWriter writer = new CSVWriter(new FileWriter("metadata//metadata.csv"));
		writer.writeAll(filteredData);
		writer.flush();
		writer.close();
		
	}
	
	

	private void validateIndexCreation(Table table, String strColName) throws DBAppException {
		// Make sure the column exists : 
		Enumeration<String> columnNames = table.getColumnNameType().keys();
		
		boolean exists = false;
		
		while(columnNames.hasMoreElements()) {
			if(columnNames.nextElement().equals(strColName)) {
				exists = true;
			}
		}
		
		if(!exists) {
			throw new DBAppException("Column Doesn't Exist in Table.");
		}
		
		
		// check if the index exists
		
		Enumeration<String> indexedCols = table.getIndices().keys();
		
		while(indexedCols.hasMoreElements()) {
			if(indexedCols.nextElement().equals(strColName)) {
				throw new DBAppException("Index on " + strColName + " already exists.");
			}
		}
		
	}
	
	private boolean tableExists(String strTableName) {
		for(String s : myTables) {
			if(s.equals(strTableName)) {
				return true;
			}
		}
		return false;
	}
	
	

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {

		Table table = getTable(strTableName);

		validateTableInsertion(table, htblColNameValue);

		table.insertTuple(htblColNameValue);

		Serializer.serializeTable(table);
	}

	
	
	// Method to get the table object using serialization and deserialization.
	public Table getTable(String strTableName) throws ClassNotFoundException, IOException {
		return Serializer.deserializeTable(strTableName);
	}
	
	

	// Method validates that the values used for insertion are correct.
	private void validateTableInsertion(Table table, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {

		// Check if the table exists in the database.
		if (!myTables.contains(table.getTableName())) {
			throw new DBAppException("Table doesn't exist.");
		}
		

		// Ensure data size is corect.
		if(!(table.getNumberOfColumns() == (htblColNameValue.size()))) {
			throw new DBAppException("You forgot to specify all the columns!");
		}
		
		
		// Read info from metadata file.
		CSVReader reader = new CSVReader(new FileReader("metadata//metadata.csv"));
		String tableName = table.getTableName();

		// Read each line for this table
		// Read all lines and add the specific columns for the needed table.
		List<String[]> tableColumns = new Vector<>();
		List<String[]> allColumns = new Vector<>();

		allColumns = reader.readAll();

		reader.close();

		for (String[] rowData : allColumns) {
			if (rowData[0].equals(tableName)) {
				tableColumns.add(rowData);
			}
		}

		int numberOfColumns = table.getNumberOfColumns();

		// Check all columns exist :
		Enumeration<String> columnsInserted = htblColNameValue.keys();

		Enumeration<String> tableCols = table.getColumnNameType().keys();

		Set<String> tableColsSet = new HashSet<>();

		// Add all columns to a hash set
		while (tableCols.hasMoreElements()) {
			tableColsSet.add(tableCols.nextElement());
		}

		// Check if all elements in columnsInserted exist in the set
		while (columnsInserted.hasMoreElements()) {
			String columnName = columnsInserted.nextElement();

			if (!(tableColsSet.contains(columnName))) {
				throw new DBAppException("Specified Column Doesn't Exist in This Table.");
			}
		}

		// Check data type integrity :
		String[] dataTypes = new String[numberOfColumns];
		String[] columnName = new String[numberOfColumns];

		// Load the third column from the tableColumns List of String Arrays for
		// datatypes.
		for (int i = 0; i < numberOfColumns; i++) {
			dataTypes[i] = tableColumns.get(i)[2];
		}

		// Load the second column from the tableColumns List of String Arrays for column
		// names.
		for (int i = 0; i < numberOfColumns; i++) {
			columnName[i] = tableColumns.get(i)[1];
		}

		for (int i = 0; i < numberOfColumns; i++) {
			Object columnValue = htblColNameValue.get(columnName[i]);

			String className = columnValue.getClass().getName();

			if (!(className.equals(dataTypes[i]))) {
				throw new DBAppException("Wrong Data Type Inserted");
			}

		}
		
		// Check if the key already exists : 
		if(table.gettablePages().size() != 0) {
			Object key = htblColNameValue.get(table.getClusteringKey());
			
			int pageNum = table.binarySearchForPage((Comparable) key);
			
			Page p = table.getPage(pageNum);
			
			if(p != null) {
				Vector<Tuple> tuples = p.getpageTuples();
				
				for(Tuple t : tuples) {
					if(t.getClusteringKey().equals(key)) {
						throw new DBAppException("Key already exists.");
					}
				}
			}
		}
		

	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {
		
		validateUpdate(strTableName, strClusteringKeyValue, htblColNameValue);

		Table t = getTable(strTableName);
		
		t.updateTuple(strClusteringKeyValue, htblColNameValue);
		
	}

	private void validateUpdate(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws ClassNotFoundException, IOException, DBAppException {
		
		// find out if the key exists in the table, if index exists on key use it, 
		// else binary search for page and then access the tuple and update it. 
		Table t = getTable(strTableName);
		
		// checks key existence, and the method checks for key type as well.
		if(!t.doesKeyExist(strClusteringKeyValue)) {
			throw new DBAppException("Key " + strClusteringKeyValue + " doesn't exist in the table.");
		}
		
		// check types from metadata : 
		CSVReader reader = new CSVReader(new FileReader("metadata//metadata.csv"));
		
		List<String[]> tableColumns = new Vector<>();
		List<String[]> allColumns = new Vector<>();

		allColumns = reader.readAll();

		reader.close();

		for (String[] rowData : allColumns) {
			if (rowData[0].equals(strTableName)) {
					tableColumns.add(rowData);
			}
		}
		
		Hashtable <String, String> colNameType = new Hashtable<>();
		
		for(String[] tableData : tableColumns) {
			colNameType.put(tableData[1], tableData[2]);
		}
		
		
		Enumeration<String> editedColumns = htblColNameValue.keys();
		
		while(editedColumns.hasMoreElements()) {
			String editedColumn = editedColumns.nextElement();
			
			if(!(colNameType.containsKey(editedColumn))) {
				throw new DBAppException("Column " + editedColumn + " doesn't exist in the table.");
			}
			
			Object editedValue = htblColNameValue.get(editedColumn);
			
			String className = editedValue.getClass().getName();
			
			String expectedClass = colNameType.get(editedColumn);
			
			if(!className.equals(expectedClass)) {
				throw new DBAppException("Incorrect data type for column " + editedColumn + " ,should be " + expectedClass + " not " + className);
			}	
		}
	
		
	}

	
	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {
		
		validateDelete(strTableName, htblColNameValue);
		
		Table t = getTable(strTableName);
		
		t.deleteTuples(htblColNameValue);
		
	}

	
	
	private void validateDelete(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {
				// Check if the table exists in the database.
		if (!myTables.contains(strTableName)) {
			throw new DBAppException("Table doesn't exist.");
		}
				

		if(htblColNameValue.size() == 0) {
			return;
		}
		
				// Read info from metadata file.
		CSVReader reader = new CSVReader(new FileReader("metadata//metadata.csv"));
				
				// Read each line for this table
				// Read all lines and add the specific columns for the needed table.
		List<String[]> tableColumns = new Vector<>();
		List<String[]> allColumns = new Vector<>();

		allColumns = reader.readAll();

		reader.close();

		for (String[] rowData : allColumns) {
			if (rowData[0].equals(strTableName)) {
					tableColumns.add(rowData);
			}
		}
				
				
				//Create a hashtable for column names/ data types.
		Hashtable <String, String> colNameType = new Hashtable<>();
				
		for(String[] tableData : tableColumns) {
			colNameType.put(tableData[1], tableData[2]);
		}
				
				
		Enumeration<String> colsToDelete = htblColNameValue.keys();
				
		while(colsToDelete.hasMoreElements()) {
			String colToDelete = colsToDelete.nextElement();
					
			if(!(colNameType.containsKey(colToDelete))) {
				throw new DBAppException("Column " + colToDelete + " specified doesn't exist.");
			}else {
						// if it exists, check the data type.
						
				Object valToDelete = htblColNameValue.get(colToDelete);
						
				String valClass = valToDelete.getClass().getName();
						
				String expectedClass = colNameType.get(colToDelete);
						
			if(!(valClass.equals(expectedClass))) {
				throw new DBAppException("Incorrect data type for " + colToDelete + " in the table.");
			}	
		}	
	}
}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException, ClassNotFoundException, IOException {

		validateSelect(arrSQLTerms, strarrOperators);
		
		Table t = getTable(arrSQLTerms[0]._strTableName);
		
		return t.selectFromTable(arrSQLTerms, strarrOperators).iterator();
	}

	private void validateSelect(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		
		// If we have more operators than expressions, throw an exception
		if(strarrOperators.length >= arrSQLTerms.length) {
			throw new DBAppException("More operators than expressions.");
		}
		
		// check that the logical operators are either AND, OR or XOR.
		for(String operator : strarrOperators) {
			if(!(operator.toLowerCase().equals("and")
				|| operator.toLowerCase().equals("or")
				|| operator.toLowerCase().equals("xor"))) 
			{
				throw new DBAppException("Invalid Logical Operation. Allowed Logical Operations are only AND, OR and XOR.");
			}
		}
		
		// check that the table exists. 
		boolean exists = false;
		
		for(int i = 0; i < arrSQLTerms.length; i++) {
			String tableName = arrSQLTerms[i]._strTableName;
			
			for(String tableNames : myTables) {
				
				if(tableNames.equals(tableName)) {
					exists = true;
				}
			}
		}
		
		if(!exists) throw new DBAppException("Table doesn't exist in database.");
		
		// check arithmetic operators are >= or <= or > or < or = or !=
		
		for(int i = 0 ; i < arrSQLTerms.length; i++) {
			String arithmeticOperator = arrSQLTerms[i]._strOperator;
			
			if(!(arithmeticOperator.equals("=")
				|| arithmeticOperator.equals(">")
				|| arithmeticOperator.equals(">=")
				|| arithmeticOperator.equals("<")
				|| arithmeticOperator.equals("<=")
				|| arithmeticOperator.equals("!="))) 
			{
				throw new DBAppException("Invalid Arithmetic Operator. Allowed Arithmetic Operators are : >, >=, < , <=, != or =");
			}
		}
	}

	public static void main(String[] args) {

		try {
			String strTableName = "Student";
			DBApp dbApp = new DBApp();

//			 Hashtable htblColNameType = new Hashtable<>();
//			 htblColNameType.put("id", "java.lang.Integer");
//			 htblColNameType.put("name", "java.lang.String");
//			 htblColNameType.put("gpa", "java.lang.Double");
//			 dbApp.createTable(strTableName, "id", htblColNameType);
////////
//			Hashtable htblColNameValue = new Hashtable();
//			htblColNameValue.put("id", 30);
//			dbApp.deleteFromTable(strTableName, htblColNameValue);
/////////
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(3));
//			htblColNameValue.put("name", new String("Karim Noor"));
//			htblColNameValue.put("gpa", new Double(1.2));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//////
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(6));
//			htblColNameValue.put("name", new String("Dalia Noor"));
//			htblColNameValue.put("gpa", new Double(1.25));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(5));
//			htblColNameValue.put("name", new String("John Noor"));
//			htblColNameValue.put("gpa", new Double(1.5));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
////
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(4));
//			htblColNameValue.put("name", new String("Zaky Noor"));
//			htblColNameValue.put("gpa", new Double(1.4));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
////			
////			htblColNameValue.clear();
////			htblColNameValue.put("id", new Integer(12));
////			htblColNameValue.put("name", new String("Peter Adel"));
////			htblColNameValue.put("gpa", new Double(0.88));
////			dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(30));
//			htblColNameValue.put("name", new String("Faizallah"));
//			htblColNameValue.put("gpa", new Double(0.88));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
////			
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(20));
//			htblColNameValue.put("name", new String("Khaled Ahmed"));
//			htblColNameValue.put("gpa", new Double(0.88));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(21));
//			htblColNameValue.put("name", new String("Sameh Fawzy"));
//			htblColNameValue.put("gpa", new Double(0.88));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(22));
//			htblColNameValue.put("name", new String("Adam Rabie"));
//			htblColNameValue.put("gpa", new Double(0.88));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
////			
////			
////
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(7));
//			htblColNameValue.put("name", new String("Ahmed Samy"));
//			htblColNameValue.put("gpa", new Double(1.2));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(8));
//			htblColNameValue.put("name", new String("Da3oush"));
//			htblColNameValue.put("gpa", new Double(1.92));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
			
			
////////			
//			htblColNameValue.clear();
//			htblColNameValue.put("name", "mohamed mohamed");
//			dbApp.updateTable(strTableName, "6", htblColNameValue);
////			
//
////////		
////////			
//			 dbApp.createIndex(strTableName, "gpa", "gpaIndex");
//
//			Vector<String> tablePages1 = Serializer.deserializeTable("Student").gettablePages();
//				
//			 for(int i = 0; i < tablePages1.size(); i++) {
//				 Page p = Serializer.deserializePage(strTableName, tablePages1.get(i));
//				 System.out.println(" Page Num : " + tablePages1.get(i));
//				 System.out.println(p);
//			 }
////		 
//			Table t1 =  Serializer.deserializeTable(strTableName);
//			Hashtable<String,BTree> indices1 = t1.getIndices();
//////			
//			BTree wantedTree1 = indices1.get("gpa");
//			wantedTree1.print();
////
//////////			
//			t1.getIndices().put("gpa", null);
//////////			
//			Serializer.serializeTable(t1);
////////			
////			
//////			
		
////			
//			ArrayList<Tuple> x = wantedTree1.searchAllOccurrences(0.88);
//			
//			for(Tuple t : x) {
//				System.out.println(t);
//			}
////				
			
//			
//			 SQLTerm[] arrSQLTerms = new SQLTerm[2];
//			 arrSQLTerms[0] = new SQLTerm();
//			 arrSQLTerms[0]._strTableName = "Student";
//			 arrSQLTerms[0]._strColumnName = "gpa";
//			 arrSQLTerms[0]._strOperator = "=";
//			 arrSQLTerms[0]._objValue = "0.88";
////////////
//			 arrSQLTerms[1] = new SQLTerm();
//			 arrSQLTerms[1]._strTableName = "Student";
//			 arrSQLTerms[1]._strColumnName = "name";
//			 arrSQLTerms[1]._strOperator = "=";
//			 arrSQLTerms[1]._objValue = "Faizallah";
//////////			 
////			 arrSQLTerms[2] = new SQLTerm();
////			 arrSQLTerms[2]._strTableName = "Student";
////			 arrSQLTerms[2]._strColumnName = "name";
////			 arrSQLTerms[2]._strOperator = "=";
////			 arrSQLTerms[2]._objValue = new String("Ahmed Noor");
////
//		   	 String[] strarrOperators = new String[1];
////////////////		
//			 strarrOperators[0] = "XOR";
////			 strarrOperators[1] = "AND";
//////////////			 
////////////		// SELECT * FROM Student WHERE name = "John Noor" OR gpa = 0.88 AND id > 10;
//			 Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//			 
//			 while(resultSet.hasNext()) {
//				 System.out.println(resultSet.next());
//			 }
			
			
			
//				BTree tree = dbApp.getTable(strTableName).getIndices().get("gpa");
//				tree.delete(1.49, 293);
//				Serializer.serializeTable(dbApp.getTable(strTableName));
		
			
		
			
			// ->>>> Have to fix removeEmptyPages.
			 
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}

}
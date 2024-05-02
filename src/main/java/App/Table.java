package App;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import com.opencsv.CSVReader;

import BTree.BTree;
import Exceptions.DBAppException;

public class Table implements Serializable {

    private static final long serialVersionUID = 4380348509385430L;
    private int pageCounter;
    private Vector<String> tablePages;
    private Hashtable<String, String> columnNameType;
    private String tableName;
    private String clusteringKey;
    private String clusteringKeyType;
    private int numberOfColumns;
    private Hashtable<String, BTree> indices;

    public Table(String tableName, String clusteringKey, Hashtable<String, String> columnNameType) {
        pageCounter = 0;
        this.tableName = tableName;
        this.clusteringKey = clusteringKey;
        this.columnNameType = columnNameType;
        clusteringKeyType = columnNameType.get(clusteringKey);
        numberOfColumns = columnNameType.keySet().size();
        indices = new Hashtable<>();

        tablePages = new Vector<>();
    }

    public Vector<String> getTablePages() {
		return tablePages;
	}

	public void setTablePages(Vector<String> tablePages) {
		this.tablePages = tablePages;
	}

	public Hashtable<String, BTree> getIndices() {
		return indices;
	}

	public void setIndices(Hashtable<String, BTree> indices) {
		this.indices = indices;
	}

	public int getNumberOfColumns() {
        return numberOfColumns;
    }

    public void setNumberOfColumns(int numberOfColumns) {
        this.numberOfColumns = numberOfColumns;
    }

    public int getPageCounter() {
        return pageCounter;
    }

    public void setPageCounter(int pageCounter) {
        this.pageCounter = pageCounter;
    }

    public Vector<String> gettablePages() {
        return tablePages;
    }

    public void settablePages(Vector<String> tablePages) {
        this.tablePages = tablePages;
    }

    public Hashtable<String, String> getColumnNameType() {
        return columnNameType;
    }

    public void setColumnNameType(Hashtable<String, String> columnNameType) {
        this.columnNameType = columnNameType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public void setClusteringKey(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public String getClusteringKeyType() {
        return clusteringKeyType;
    }

    public void setClusteringKeyType(String clusteringKeyType) {
        this.clusteringKeyType = clusteringKeyType;
    }

    public void insertTuple(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException {
    	//removeEmptyPagesFromTable();
        // Create a tuple :
        Tuple newTuple = new Tuple(htblColNameValue, clusteringKey);

        Page currPage;

        removeEmptyPagesFromTable();
        
        if (tablePages.size() == 0) {
            currPage = createNewPage();
            currPage.addRowToPage(newTuple);
            return;
        } else {
            // binary search for page :

            int pageIndex = binarySearchForPage((Comparable) newTuple.getClusteringKey());
            currPage = getPage(pageIndex);

            if (currPage.isFull()) {
                // check if it's the last page :
                if (pageIndex == tablePages.size() - 1) {

                    // Shift one row down
                    currPage.addRowToPage(newTuple);
                    Tuple lastTuple = currPage.removeLastTuple();

                    // Create new page to insert the last row
                    Page newPage = createNewPage();
                    newPage.addRowToPage(lastTuple);
                    
                    updateReferenceTuples(lastTuple, Integer.parseInt(lastTuple.getPageNum()));                    
                } else {
                	// Look for the next free page in the table
                    Page nextFreePage = getnextFreePage(pageIndex, currPage, newTuple);
             
                    pageIndex++;
                    Page nxtPage = getPage(pageIndex);

                    currPage.addRowToPage(newTuple);
                    Tuple lastTuple = currPage.removeLastTuple();
                    
                    while (!(nextFreePage.getPageNum().equals(nxtPage.getPageNum()))) {
                        currPage = nxtPage;
                        pageIndex++;
                        nxtPage = getPage(pageIndex);
                        currPage.addRowToPage(lastTuple);
                        
                        // new line
                        updateReferenceTuples(lastTuple, Integer.parseInt(lastTuple.getPageNum()));
                        
                        
                        lastTuple = currPage.removeLastTuple();
                    }
                    
                    nextFreePage.addRowToPage(lastTuple);
                    
                    // new line
                    updateReferenceTuples(lastTuple, Integer.parseInt(lastTuple.getPageNum()));
                    
                }
            } else {
                currPage.addRowToPage(newTuple);
            }
        }
        populateIndices(newTuple);
        Serializer.serializeTable(this);
    }

    private Page getnextFreePage(int pageIndex, Page currPage, Tuple newTuple)
            throws IOException, ClassNotFoundException {
    	
    	//get the next page
        Page nextPage = getPage(++pageIndex);

        
        while (nextPage.isFull()) {
        	// if the next page is full and it's the last page, else return the next page if it's not full
            if (nextPage.getPageNum().equals(tablePages.get(tablePages.size() -1))) {
                Page newPage = createNewPage();
                return newPage;
            }
            nextPage = getPage(++pageIndex);
        }
        return nextPage;

    }

    public Page getPage(int pageIndex) throws ClassNotFoundException, IOException {
        // to deserialize the page.
        String pageName = tablePages.get(pageIndex);
        Page page = Serializer.deserializePage(tableName, pageName);
        return page;
    }

    public void removeEmptyPagesFromTable() throws ClassNotFoundException, IOException {
       Iterator<String> pageNames = tablePages.iterator();
    	while(pageNames.hasNext()) {
    		
        	Page page = getPage(Integer.parseInt(pageNames.next()));
        	
        	if(page.getpageTuples().size() == 0) {
        		pageNames.remove();
//        		tablePages.remove(page.getPageNum());
        		String filePath = Serializer.getFilePath(tableName, page.getPageNum());
        		File file = new File(filePath);
        		
        		if(file.exists()) {
        			file.delete();
        		}
        		
        		
        		renameAllFilesAndPages();
        		pageCounter--;
        		
        		Serializer.serializeTable(this);
        	}
        }
    }

    private void renameAllFilesAndPages() throws NumberFormatException, ClassNotFoundException, IOException {

    	for(int i = 0; i < tablePages.size(); i++) {
    		
    		String oldNum = tablePages.get(i);
    		
    		Page page = Serializer.deserializePage(tableName, oldNum);
    		
    		page.setPageNum(i + "");
    		
    		
    		String filePath = Serializer.getFilePath(tableName, oldNum);
    		File deleteOldFile = new File(filePath);
    		
    		if(deleteOldFile.exists()) {
    			deleteOldFile.delete();
    		}
    		
    		for(Tuple t : page.getpageTuples()) {
    			t.setPageNum(i + "");
    			updateReferenceTuples(t, i);
    		}
    		
    		tablePages.set(i, i + "");
    		
    		Serializer.serializePage(page.getPageNum() + "", page);
    		
    	}
    
    	Serializer.serializeTable(this);
	}

	private void updateReferenceTuples(Tuple t, int i) {
		Enumeration<String> indexedCols = indices.keys();
		
		while(indexedCols.hasMoreElements()) {
			String indexedCol = indexedCols.nextElement();
			
			BTree indexTree = indices.get(indexedCol);
			
			
			if(indexTree.search( (Comparable) t.getValueOfColumn(indexedCol)) instanceof ArrayList) {
				ArrayList<TupleReference> tmpRef = (ArrayList) indexTree.search((Comparable) t.getValueOfColumn(indexedCol));
				
				for(TupleReference tupRef : tmpRef) {
					if(tupRef.getClusteringKey().equals(t.getClusteringKey())) {
						tmpRef.remove(tupRef);
						break;
					}
				}
				
			}else {
				indexTree.delete( (Comparable) t.getValueOfColumn(indexedCol), t.getClusteringKey());

			}
			
			TupleReference newRef = new TupleReference(t.getClusteringKey(), t.getPageNum());
			
			indexTree.insert((Comparable) t.getValueOfColumn(indexedCol), newRef);
		}
		
	}

	public int binarySearchForPage(Comparable primaryKey) throws ClassNotFoundException, IOException {
        int size = tablePages.size();
        
        int lo = 0;
        int hi = size - 1;

        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            Page currPage = Serializer.deserializePage(tableName, tablePages.get(mid));

            Comparable firstKeyInPage = (Comparable) currPage.getfirstClusteringKey();
            Comparable lastKeyInPage = (Comparable) currPage.getlastClusteringKey();

            int compareWithFirst = primaryKey.compareTo(firstKeyInPage);
            int compareWithLast = primaryKey.compareTo(lastKeyInPage);

            if (compareWithLast <= 0 && compareWithFirst >= 0) {
                return mid;
            } else if (compareWithLast > 0) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        if (lo < size - 1) {
            return lo;
        } else {
            return size - 1;
        }
    }

    public Page createNewPage() throws IOException {
        Page page = new Page(tableName, (pageCounter++) + "");
        Serializer.serializePage(page.getPageNum() + "", page);
        tablePages.add(page.getPageNum());
        return page;
    }
    
    public void populateIndices(Tuple t) throws ClassNotFoundException, IOException {
    	Enumeration<String> indexedColumns = indices.keys();
    	
    	
    	while(indexedColumns.hasMoreElements()) {
    		String indexedColumn = indexedColumns.nextElement();
    		
//    		if(indices.get(indexedColumns) == null) {
//        		indices.put(indexedColumn, new BTree());
//        	}
    		
    		BTree tree = indices.get(indexedColumn);
    		
    		Object columnVal = t.getValueOfColumn(indexedColumn);
    		
    		TupleReference tupleRef = new TupleReference(t.getClusteringKey(), t.getPageNum());
    		
    		tree.insert((Comparable) columnVal, tupleRef);
    		
    	}
    }

	public Vector<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws NumberFormatException, ClassNotFoundException, IOException {
		
		// create Vector<Vector<Tuple>> , each Vector<Tuple> has the selections, apply the logical operations on each. 
		
		
		Vector<Vector<Tuple>> selections = new Vector<>();
		
		for(int i = 0; i < arrSQLTerms.length; i++) {
			String columnName = arrSQLTerms[i]._strColumnName;
			String arithmeticOperation = arrSQLTerms[i]._strOperator;
			Object value = arrSQLTerms[i]._objValue;
			
			String classType = columnNameType.get(columnName);
			
			Object castedValue = castToClass(value, classType);
			
			if(indexExistsOnColumn(columnName)) {
				selections.add(indexSelect(columnName, arithmeticOperation, castedValue));
			}else {
				selections.add(linearSearch(columnName, arithmeticOperation, castedValue));
			}
		}
		
		
		Vector<Tuple> results = applyLogicalOperations(selections, strarrOperators);
		
		
		return selections.get(0);
	}

	private Vector<Tuple> applyLogicalOperations(Vector<Vector<Tuple>> selections, String[] strarrOperators) {
		Vector<Tuple> results = new Vector<>();
		
		int i = 0, j = 0;
		while(selections.size() > 1) {
			Vector<Tuple> firstSet = selections.remove(i);
			Vector<Tuple> secondSet = selections.remove(i);
			
			
			
			String operation = strarrOperators[j++];
			
			switch(operation) {
			
			case "AND" :
				results = applyAndOp(firstSet,secondSet);
				break;
			
			case "OR" :
				results = applyOrOp(firstSet, secondSet);
				break;
				
			
			case "XOR" : 
				results = applyXorOp(firstSet, secondSet);
				break;
			}
			
			selections.add(0, results);
			
		}
		
		return selections.get(0);
	}
	
	
	private Vector<Tuple> applyXorOp(Vector<Tuple> firstSet, Vector<Tuple> secondSet) {
		Vector<Tuple> results = new Vector<>();
		for(Tuple t1 : firstSet) {
			for(Tuple t2 : secondSet) {
				if(!(t1.getClusteringKey().equals(t2.getClusteringKey()))) {
					results.add(t1);
				}
			}
		}
		
		return results;
	}

	public Vector<Tuple> applyAndOp(Vector<Tuple> firstSet, Vector<Tuple> secondSet){
		Vector<Tuple> results = new Vector<>();
		for(Tuple t1 : firstSet) {
			for(Tuple t2 : secondSet) {
				if(t1.getClusteringKey().equals(t2.getClusteringKey())) {
					results.add(t1);
				}
			}
		}
		
		return results;
	}
	
	public Vector<Tuple> applyOrOp(Vector<Tuple> firstSet, Vector<Tuple> secondSet){
		Vector<Tuple> results = new Vector<>();
		results.addAll(firstSet);
		
		for (Tuple t1 : secondSet) {
	        boolean found = false;
	        for (Tuple t2 : results) {
	            if (t1.getClusteringKey().equals(t2.getClusteringKey())) {
	                found = true;
	                break;
	            }
	        }
	        if (!found) {
	            results.add(t1);
	        }
	    }
		
		return results;
	}

	private Object castToClass(Object value, String classType) {
		Object newVal = null;
		
		switch(classType) {
		case "java.lang.Integer" :
			newVal = Integer.parseInt(String.valueOf(value));
			return newVal;
		case "java.lang.Double" : 
			newVal = Double.parseDouble(String.valueOf(value));
			return newVal;
		case "java.lang.String" : 
			newVal = String.valueOf(value);
			return newVal;
		}
		
		return newVal;
		
	}

	private Vector<Tuple> indexSelect(String columnName, String arithmeticOperation, Object value) throws NumberFormatException, ClassNotFoundException, IOException {
		BTree treeIndex = indices.get(columnName);
		Vector<Tuple> searchResults = new Vector<>();
		ArrayList<Tuple> tmp = new ArrayList<>();
		ArrayList<TupleReference> tmpRef = new ArrayList<>();
		TupleReference tupRef;
		
		if(!(columnName.equals(clusteringKey))) {
			// if not on clustering key, we may have duplicates, extra cases needed.
			return duplicateIndexSelect(columnName, arithmeticOperation, value, treeIndex);
		}else {
			switch(arithmeticOperation) {
			
			case "=" : 
				tupRef = (TupleReference) treeIndex.search((Comparable) value);
				Tuple t = getTuple(tupRef);
				searchResults.add(t);
				break;
				
			case ">" :
				tmpRef = treeIndex.searchGreaterThan((Comparable) value);
				tmp = getTuples(tmpRef);
				searchResults.addAll(tmp);
				break;
				
			case ">=" : 
				tupRef = (TupleReference) treeIndex.search((Comparable) value);
				tmpRef = treeIndex.searchGreaterThan((Comparable) value);
				tmp = getTuples(tmpRef);
				tmp.add(getTuple(tupRef));
				searchResults.addAll(tmp);
				break;
				
			case "<" :
				tmpRef = treeIndex.searchLessThan( (Comparable) value);
				tmp = getTuples(tmpRef);

				searchResults.addAll(tmp);
				break;
				
			case "<=" :
				tupRef = (TupleReference) treeIndex.search((Comparable) value);
				tmpRef = treeIndex.searchLessThan((Comparable) value);
				tmp = getTuples(tmpRef);
				tmp.add(getTuple(tupRef));
				searchResults.addAll(tmp);
				break;
				
			case "!=" :
				// No need to loop on the BTree nodes. We will have to check every page regardless.
				searchResults = linearSearch(columnName, arithmeticOperation, value);
				break;
			}
		}
		
		return searchResults;
		
	}

	private Tuple getTuple(TupleReference tupRef) throws NumberFormatException, ClassNotFoundException, IOException {
		String pageNum = tupRef.getPageNum();
		
		Page p = getPage(Integer.parseInt(pageNum));
		
		Tuple t = p.binarySearchInPage(tupRef.getClusteringKey());
		
		return t;
		
	}

	private ArrayList<Tuple> getTuples(ArrayList<TupleReference> tupRefs) throws NumberFormatException, ClassNotFoundException, IOException {
		ArrayList<Tuple> resultSet = new ArrayList<>();
		
		for(TupleReference tupRef : tupRefs) {
			String pageNum = tupRef.getPageNum();
			
			Page p = getPage(Integer.parseInt(pageNum));
			
			Tuple t = p.binarySearchInPage(tupRef.getClusteringKey());
			
			resultSet.add(t);
		}
		
		return resultSet;
		
	}

	private Vector<Tuple> duplicateIndexSelect(String columnName, String arithmeticOperation, Object value, BTree indexTree) throws NumberFormatException, ClassNotFoundException, IOException {
		Vector<Tuple> searchResults = new Vector<>();
		ArrayList<Tuple> tmp1 = new ArrayList<>();
		ArrayList<Tuple> tmp2 = new ArrayList<>();
		
		ArrayList<TupleReference> tmpRef1 = new ArrayList<>();
		ArrayList<TupleReference> tmpRef2 = new ArrayList<>();
		
		TupleReference tupRef;
		
		switch(arithmeticOperation) {
		
		case "=" : 
			tmpRef1 = indexTree.searchAllOccurrences((Comparable) value);
			
			tmp1 = getTuples(tmpRef1);
			
			searchResults.addAll(tmp1);
			break;
		
		case ">" :
			tmpRef1 = indexTree.searchGreaterThan((Comparable) value);
			
			tmp1 = getTuples(tmpRef1);
			
			searchResults.addAll(tmp1);
			break;
			
		case ">=" : 
			tmpRef1 = indexTree.searchAllOccurrences((Comparable) value);
			tmpRef2 = indexTree.searchGreaterThan((Comparable) value);
			
			tmp1 = getTuples(tmpRef1);
			tmp2 = getTuples(tmpRef2);
			
			
			searchResults.addAll(tmp1);
			searchResults.addAll(tmp2);
			break;
			
		case "<" : 
			tmpRef1 = indexTree.searchLessThan((Comparable) value);
			tmp1 = getTuples(tmpRef1);
			
			searchResults.addAll(tmp1);
			break;
			
		case "<=" :
			tmpRef1 = indexTree.searchAllOccurrences((Comparable) value);
			tmpRef2 = indexTree.searchLessThan((Comparable) value);
			
			tmp1 = getTuples(tmpRef1);
			tmp2 = getTuples(tmpRef2);
			
			
			searchResults.addAll(tmp1);
			searchResults.addAll(tmp2);
			break;
			
		case "!=" : 	
			searchResults = linearSearch(columnName, arithmeticOperation, value);
			break;
		}
		
		return searchResults;
	
	}

	private Vector<Tuple> linearSearch(String columnName, String arithmeticOperation, Object value) throws NumberFormatException, ClassNotFoundException, IOException {
		Vector<Tuple> searchResults = new Vector<>();
		
		//Iterate over table pages.
		for(String s : tablePages) {
			Page p = getPage(Integer.parseInt(s));
			
			//Iterate over page tuples.
			for(Tuple t : p.getpageTuples()) {
				Object tupleVal = t.getValueOfColumn(columnName);
				
				if(operatorSelection((Comparable) tupleVal, (Comparable) value, arithmeticOperation)) {
					searchResults.add(t);
				}
			}
		}
		
		return searchResults;
		
	}
	
	
	

	private boolean indexExistsOnColumn(String columnName) {
		Enumeration<String> indexedColumns = indices.keys();
		
		// Check if column name exists in the indices hashtable.
		while(indexedColumns.hasMoreElements()) {
			String col = indexedColumns.nextElement();
			
			if(col.equals(columnName)) {
				return true;
			}
		}
		return false;
	}

	private boolean operatorSelection(Comparable firstOperand, Comparable secondOperand, String operator) {

		// could change to switch, same thing. 
		if (operator.equals("="))
			return firstOperand.compareTo(secondOperand) == 0;
		if (operator.equals(">"))
			return firstOperand.compareTo(secondOperand) > 0;
		if (operator.equals(">="))
			return firstOperand.compareTo(secondOperand) >= 0;
		if (operator.equals("<"))
			return firstOperand.compareTo(secondOperand) < 0;
		if (operator.equals("<="))
			return firstOperand.compareTo(secondOperand) <= 0;
		else
			return firstOperand.compareTo(secondOperand) != 0;
	}

	public void deleteTuples(Hashtable<String, Object> htblColNameValue) throws NumberFormatException, ClassNotFoundException, IOException {

		if(htblColNameValue.isEmpty()) {
			deleteAllTuples();
			return;
		}
		
		// transform the conditions into queries. "in RBDMS, the conditions specified in the delete statement are typically transformed into a query that retrieves the tuples to be deleted"
		
		SQLTerm[] queriesForRowsToDelete = queryBuilder(htblColNameValue);
		String[] arrOperators = new String[queriesForRowsToDelete.length - 1];
		
		// All conditions are "ANDED" together
		for(int i = 0; i < arrOperators.length; i++) {
			arrOperators[i] = "AND";
		}
		
		
		// the select uses indices specified, so in turn the delete will use the indices.
		Vector<Tuple> rowsToDelete = selectFromTable(queriesForRowsToDelete, arrOperators);
		
		for(Tuple t : rowsToDelete) {
			String pageNum = t.getPageNum();
			Page p = getPage(Integer.parseInt(pageNum));
			
			p.removeTuple(t);
			
		}
		
		deleteFromIndices(rowsToDelete);
		
		removeEmptyPagesFromTable();
		
		Serializer.serializeTable(this);
	}

	private void deleteAllTuples() throws NumberFormatException, ClassNotFoundException, IOException {
		for(String s : tablePages) {
			Page p = getPage(Integer.parseInt(s));
			
			p.deleteAllTuples();
			
			Serializer.serializePage(p.getPageNum(), p);
		}
		
		// delete all indices
		indices.clear();
		
		removeEmptyPagesFromTable();
		
		Serializer.serializeTable(this);
	}

	private void deleteFromIndices(Vector<Tuple> rowsToDelete) throws ClassNotFoundException, IOException {
		Enumeration<String> indexedCols = indices.keys();
		
		
		while(indexedCols.hasMoreElements()) {
			String indexingCol = indexedCols.nextElement();
			
			BTree indexTree = indices.get(indexingCol);
		
			for(Tuple t : rowsToDelete) {
				
				if(indexTree.search((Comparable) t.getValueOfColumn(indexingCol)) instanceof ArrayList) {
					ArrayList<TupleReference> tmp = (ArrayList) indexTree.search((Comparable)t.getValueOfColumn(indexingCol));
					
					for(TupleReference tmpRef : tmp) {
						if(tmpRef.getClusteringKey().equals(t.getClusteringKey())) {
							tmp.remove(tmpRef);
							break;
						}
					}
					
					
				}else {
				
					indexTree.delete( (Comparable) t.getValueOfColumn(indexingCol), t.getClusteringKey());
				}
			}
		}	
		
        removeEmptyPagesFromTable();
	}

	private SQLTerm[] queryBuilder(Hashtable<String, Object> htblColNameValue) {
		SQLTerm[] queries = new SQLTerm[htblColNameValue.size()];
		Enumeration<String> colNames = htblColNameValue.keys();
		
		for(int i = 0; i < htblColNameValue.size(); i++) {
			queries[i] = new SQLTerm();
			queries[i]._strTableName = tableName;
			queries[i]._strColumnName = colNames.nextElement();
			queries[i]._strOperator = "=";
			queries[i]._objValue = htblColNameValue.get(queries[i]._strColumnName);
		}
		
		return queries;
		
	}

	public boolean doesKeyExist(String clusteringKey) throws ClassNotFoundException, IOException, DBAppException {
		
		Object key;
		
		try{
			 key = castToClass(clusteringKey, clusteringKeyType);
		}catch(ClassCastException e) {
			throw new DBAppException("Incorrect data type of key.");
		}
		
		// if index exists, search using the index
		if(indices.get(clusteringKey) != null) {
			BTree keyIndex = indices.get(clusteringKey);
			
			Tuple t = (Tuple) keyIndex.search( (Comparable) key);
			
			if(t != null) {
				return true;
			}
		}
		
		// else, search using binary search 
		
		int pageNum = binarySearchForPage( (Comparable ) key);
				
		if(tablePages.contains(pageNum + "")) {
			
			Page p = getPage(pageNum);
			
			Vector<Tuple> pageTuples = p.getpageTuples();
			
			for(Tuple t : pageTuples) {
				if(t.getClusteringKey().equals(key)) {
					return true;
				}
			}
			
		}else {
			return false;
		}
		
		return false;
	}

	public void updateTuple(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws NumberFormatException, ClassNotFoundException, IOException, DBAppException {
		Object key = castToClass(strClusteringKeyValue, clusteringKeyType);
		
		Hashtable<String, Object> selectHtbl = new Hashtable<>();
		
		selectHtbl.put(clusteringKey, key);
		
		SQLTerm[] updateQuery = queryBuilder(selectHtbl);
		String[] Ops = new String[0];
		
		Vector<Tuple> tmp = selectFromTable(updateQuery, Ops);
		
		if(tmp.size() > 1) {
			throw new DBAppException("Duplicate key found. Make sure it is defined correctly in creation.");
		}else {
			
			Tuple toEdit = tmp.get(0);
			
			String pageNum = toEdit.getPageNum();
			
			Page p = getPage(Integer.parseInt(pageNum));
			
			Hashtable<String, Object> oldVals = toEdit.getColumnValues();
			
			Hashtable<String, Object> modifiedVals = new Hashtable<>();
			
			Enumeration<String> valsToEdit = htblColNameValue.keys();
			
			while(valsToEdit.hasMoreElements()) {
				String editedCol = valsToEdit.nextElement();
				
				modifiedVals.put(editedCol, oldVals.get(editedCol));
				
				Object editedVal = htblColNameValue.get(editedCol);
				
				oldVals.put(editedCol, editedVal);
			}
			
			toEdit.setColumnValues(oldVals);
			
			
			p.removeTupleKey(key);

			
			
			p.addRowToPage(toEdit);
			updateIndices(modifiedVals, oldVals, key, toEdit);
			
			
			Serializer.serializePage(pageNum, getPage(Integer.parseInt(pageNum)));
			
			Serializer.serializeTable(this);
			
		}
		
	}

	

	private void updateIndices(Hashtable<String, Object> modifiedVals, Hashtable<String, Object> newVals, Object key, Tuple t) throws IOException {
		// if the index doesn't exist regardless, we want to change the tuple itself. 
	
		// if index exists, and is in modified vals, we want to change both the key and the tuple.
		
		Enumeration<String> indexedCols = indices.keys();
		TupleReference tmpRef = new TupleReference(t.getClusteringKey(), t.getPageNum());
		
		
		
		while(indexedCols.hasMoreElements()) {
			// get the indexed column.
			String indexedCol = indexedCols.nextElement();
			
			// get the corresponding btree.
			BTree indexTree = indices.get(indexedCol);
			
			if(modifiedVals.containsKey(indexedCol)) {
				
				Object valToModify = modifiedVals.get(indexedCol);
				
				if(indexTree.search( (Comparable) valToModify) instanceof ArrayList) {
					ArrayList<TupleReference> tmp = (ArrayList) indexTree.search((Comparable) valToModify);
					
					for(TupleReference tupRef : tmp) {
						if(tupRef.getClusteringKey().equals(key)) {
							tmp.remove(tupRef);
							break;
						}
					}
					
				}else {
					indexTree.delete( (Comparable) modifiedVals.get(indexedCol), tmpRef.getClusteringKey());
				}
				
				
				indexTree.insert((Comparable) newVals.get(indexedCol), tmpRef);
				
			}
				
		}
		Serializer.serializeTable(this);
	}
	
}

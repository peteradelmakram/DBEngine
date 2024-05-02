package App;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {

    private static final long serialVersionUID = 3945092534853L;
    private String pageNum;
    private Vector<Tuple> pageTuples;
    private Object firstClusteringKey, lastClusteringKey;
    private String tableName;
    private int maxRows;

    public Page(String tableName, String pageNum) throws IOException {
        this.tableName = tableName;
        pageTuples = new Vector<>();
        this.pageNum = pageNum;

        // Read max number of rows from config file
        Properties properties = new Properties();
        InputStream inputStream = Page.class.getClassLoader().getResourceAsStream("DBApp.config");
        properties.load(inputStream);
        maxRows = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));

    }

    public String getPageNum() {
        return pageNum;
    }

    public void setPageNum(String pageNum) {
        this.pageNum = pageNum;
    }

    public Vector<Tuple> getpageTuples() {
        return pageTuples;
    }

    public void setpageTuples(Vector<Tuple> pageTuples) {
        this.pageTuples = pageTuples;
    }

    public Object getfirstClusteringKey() {
        return firstClusteringKey;
    }

    public void setfirstClusteringKey(Object firstClusteringKey) {
        this.firstClusteringKey = firstClusteringKey;
    }

    public Object getlastClusteringKey() {
        return lastClusteringKey;
    }

    public void setlastClusteringKey(Object lastClusteringKey) {
        this.lastClusteringKey = lastClusteringKey;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public boolean isEmpty() {
        return pageTuples.size() == 0;
    }

    public boolean isFull() {
        // Used for insertion and deletion.
        return pageTuples.size() == maxRows;
    }

    public void updatePageKeys() {
        // Used for binary search in the table to find page to insert in.
        if (pageTuples.size() == 0) {
            firstClusteringKey = pageTuples.get(0).getClusteringKey();
            lastClusteringKey = pageTuples.get(0).getClusteringKey();
        } else {
            firstClusteringKey = pageTuples.get(0).getClusteringKey();
            lastClusteringKey = pageTuples.get(pageTuples.size() - 1).getClusteringKey();
        }
    }

    public void addRowToPage(Tuple newRow) throws IOException {
        pageTuples.add(newRow);
        
        pageTuples.sort(new Comparator<Tuple>() {

            public int compare(Tuple t1, Tuple t2) {
                Comparable c1 = (Comparable) t1.getClusteringKey();
                Comparable c2 = (Comparable) t2.getClusteringKey();

                return c1.compareTo(c2);
            }

        });
        
        newRow.setPageNum(pageNum);
        
        
        updatePageKeys();
        Serializer.serializePage(pageNum + "", this);
    }

    public Tuple removeLastTuple() throws IOException {
    	Tuple lastTuple = pageTuples.remove(pageTuples.size() - 1);
    	updatePageKeys();
    	Serializer.serializePage(pageNum + "", this);
    	
    	return lastTuple;
    }
    
    
    public String toString() {
        String s = "";

        for (Tuple t : pageTuples) {
            s += t.toString() + "\n";
        }

        return s + "\n";
    }

	public void removeTuple(Tuple tuple) throws IOException {
		Tuple toRemove = null;
		for(Tuple t : pageTuples) {
			if(t.getClusteringKey().equals(tuple.getClusteringKey())) {
				toRemove = t;
			}
		}
		
		pageTuples.remove(toRemove);
		
		Serializer.serializePage(pageNum + "", this);
		
		if(pageTuples.size() != 0) {
			updatePageKeys();
		}
	}

	public void removeTupleKey(Object key) throws IOException {
		Tuple toRemove = null;
		for(Tuple t : pageTuples) {
			if(t.getClusteringKey().equals(key)) {
				toRemove = t;
			}
		}
		
		pageTuples.remove(toRemove);
		
		Serializer.serializePage(pageNum + "", this);
		
		if(pageTuples.size() != 0) {
			updatePageKeys();
		}
	}

	public Tuple binarySearchInPage(Object clusteringKey) {
		int low = 0;
        int high = pageTuples.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple midTuple = pageTuples.get(mid);
            Comparable midKey = (Comparable) midTuple.getClusteringKey();

            if (midKey.equals(clusteringKey)) {
                return midTuple;
            } else if (midKey.compareTo(clusteringKey) < 0) {
                low = mid + 1; 
            } else {
                high = mid - 1;
            }
        }
        
        return null;
	}

	public void deleteAllTuples() {
		pageTuples.clear();
	}

}

package App;

import java.io.Serializable;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;

public class Tuple implements Serializable {
    private static final long serialVersionUID = 292509825903489L;
    private Hashtable<String, Object> columnValues;
    private Object clusteringKey;
    private String clusteringKeyName;
  
// Needs to be updated every now and then. Review insertion and deletion cases. 
    private String pageNum;

    public String getPageNum() {
		return pageNum;
	}

	public void setPageNum(String pageNum) {
		this.pageNum = pageNum;
	}

	public Tuple(Hashtable<String, Object> columnValues, String clusteringKeyName) {
        this.columnValues = columnValues;
        this.clusteringKeyName = clusteringKeyName;
        this.clusteringKey = columnValues.get(clusteringKeyName);
    }

    public Object getValueOfColumn(String columnName) {
        return columnValues.get(columnName);
    }

    public Hashtable<String, Object> getColumnValues() {
        return columnValues;
    }

    public void setColumnValues(Hashtable<String, Object> columnValues) {
        this.columnValues = columnValues;
    }

    public Object getClusteringKey() {
        return clusteringKey;
    }

    public void setClusteringKey(Object clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public String getClusteringKeyName() {
        return clusteringKeyName;
    }

    public void setClusteringKeyName(String clusteringKeyName) {
        this.clusteringKeyName = clusteringKeyName;
    }

    public String toString() {
        String s = " ";
        String temp = "";

        Enumeration<String> colNames = columnValues.keys();

        while (colNames.hasMoreElements()) {
            temp = columnValues.get(colNames.nextElement()).toString();

            if (!(colNames.hasMoreElements())) {
                s += temp;
            } else {
                s += temp + " , ";
            }
        }

        return s;
    }

}
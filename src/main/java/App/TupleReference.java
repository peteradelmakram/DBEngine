package App;

import java.io.Serializable;

// This acts as a pointer to the tuple itself for the BTree. It holds the key and the pagenum
public class TupleReference implements Serializable{
	
    private static final long serialVersionUID = 4859289582795725L;
	Object clusteringKey;
	String pageNum;
	
	public TupleReference(Object clusteringKey, String pageNum) {
		this.clusteringKey = clusteringKey;
		this.pageNum = pageNum;
	}
	public Object getClusteringKey() {
		return clusteringKey;
	}

	public void setClusteringKey(Object clusteringKey) {
		this.clusteringKey = clusteringKey;
	}

	public String getPageNum() {
		return pageNum;
	}

	public void setPageNum(String pageNum) {
		this.pageNum = pageNum;
	}
	
	public String toString() {
		return "(" + clusteringKey + " , " + pageNum + ")";
	}
}


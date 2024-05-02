package BTree;
import App.TupleReference;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * A B+ tree Since the structures and behaviors between internal node and
 * external node are different, so there are two different classes for each kind
 * of node.
 *
 * @param < TKey > the data type of the key
 * @param < TValue > the data type of the value
 */
public class BTree<TKey extends Comparable<TKey> , TValue> implements Serializable {
    /**
     * @uml.property name="root"
     * @uml.associationEnd multiplicity="(1 1)"
     */
    private BTreeNode<TKey> root;
    private static final long serialVersionUID = 5304953243098095039L;
    /**
     * @uml.property name="tableName"
     */
    private String tableName;

    public BTree() {
        this.root = new BTreeLeafNode<TKey, TValue>();
    }

    /**
     * Insert a new key and its associated value into the B+ tree.
     */
    public void insert(TKey key, TValue value) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        leaf.insertKey(key, value);

        if (leaf.isOverflow()) {
            BTreeNode<TKey> n = leaf.dealOverflow();
            if (n != null)
                this.root = n;
        }
    }

    /**
     * Search a key value on the tree and return its associated value.
     */
    public TValue search(TKey key) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        int index = leaf.search(key);
        return (index == -1) ? null : leaf.getValue(index);
    }

    /**
     * Delete a key and its associated value from the tree.
     */
    public boolean delete(TKey key, Object clusteringKey) {
    	
    	  BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
    	    int index = leaf.search(key);
    	    if (index == -1)
    	        return false;

    	    Object value = leaf.getValue(index);
    	    
    	    // If the value is an ArrayList, handle it accordingly
    	    if (value instanceof ArrayList) {
    	        ArrayList<TValue> valueList = (ArrayList<TValue>) value;
    	        
    	        // Iterate through the ArrayList to find the tuple with the specified clustering key
    	        for (int i = 0; i < valueList.size(); i++) {
    	            TupleReference tuple = (TupleReference) valueList.get(i);
    	            if (tuple.getClusteringKey().equals(clusteringKey)) {
    	                // Found the tuple, remove it from the ArrayList
    	                valueList.remove(i);
    	                
    	                // If the ArrayList is empty, remove it from the leaf node
    	                if (valueList.isEmpty()) {
    	                    leaf.deleteAt(index);
    	                    if (leaf.isUnderflow()) {
    	                        BTreeNode<TKey> n = leaf.dealUnderflow();
    	                        if (n != null)
    	                            this.root = n;
    	                    }
    	                }
    	                
    	                return true; // Deleted successfully
    	            }
    	        }
    	        
    	        // Tuple with the specified clustering key not found in the ArrayList
    	        return false;
    	    } else {
    	    	   // Handle the case when the value is not an ArrayList (normal deletion)
    	        TupleReference tuple = (TupleReference) value;
    	        if (tuple.getClusteringKey().equals(clusteringKey)) {
    	            // Found the tuple, remove it from the leaf node
    	            leaf.deleteAt(index);
    	            
    	            if (leaf.isUnderflow()) {
    	                BTreeNode<TKey> n = leaf.dealUnderflow();
    	                if (n != null)
    	                    this.root = n;
    	            }
    	            
    	            return true; // Deleted successfully
    	        } else {
    	            // Tuple with the specified clustering key not found
    	            return false;
    	        }
    	    
    	   }
//        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
//
//        if (leaf.delete(key) && leaf.isUnderflow()) {
//            BTreeNode<TKey> n = leaf.dealUnderflow();
//            if (n != null)
//                this.root = n;
//        }
    	
//    	 BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
//    	    int index = leaf.search(key);
//    	    if (index == -1)
//    	        return false;
//
//    	    // Find the index of the tuple with the specified clustering key
//    	    int tupleIndex = -1;
//    	    for (int i = 0; i < leaf.getKeyCount(); i++) {
//    	        TupleReference tuple = (TupleReference) leaf.getValue(i);
//    	        if (leaf.getKey(i).equals(key) && tuple.getClusteringKey().equals(clusteringKey)) {
//    	            tupleIndex = i;
//    	            break;
//    	        }
//    	    }
//
//    	    if (tupleIndex == -1)
//    	        return false; // Tuple with the specified clustering key not found
//
//    	    leaf.deleteAt(tupleIndex);
//    	    
//            if (leaf.isUnderflow()) {
//              BTreeNode<TKey> n = leaf.dealUnderflow();
//              if (n != null)
//                  this.root = n;
//            }
//
//    	    return true;
    }


    
    /**
     * Search the leaf node which should contain the specified key
     */
    @SuppressWarnings("unchecked")
    private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
        BTreeNode<TKey> node = this.root;
        while (node.getNodeType() == TreeNodeType.InnerNode) {
            node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
        }

        return (BTreeLeafNode<TKey, TValue>) node;
    }

    public void print() {
        ArrayList<BTreeNode> upper = new ArrayList<>();
        ArrayList<BTreeNode> lower = new ArrayList<>();

        upper.add(root);
        while (!upper.isEmpty()) {
            BTreeNode cur = upper.get(0);
            if (cur instanceof BTreeInnerNode) {
                ArrayList<BTreeNode> children = ((BTreeInnerNode) cur).getChildren();
                for (int i = 0; i < children.size(); i++) {
                    BTreeNode child = children.get(i);
                    if (child != null)
                        lower.add(child);
                }
            }
            System.out.println(cur.toString() + " ");
            upper.remove(0);
            if (upper.isEmpty()) {
                System.out.println("\n");
                upper = lower;
                lower = new ArrayList<>();
            }
        }
    }

    public BTreeLeafNode getSmallest() {
        return this.root.getSmallest();
    }

    public String commit() {
        return this.root.commit();
    }
    
    public ArrayList<TValue> searchGreaterThan(TKey minValue) {
        ArrayList<TValue> result = new ArrayList<>();
        BTreeNode<TKey> leaf = findLeafNodeShouldContainKey(minValue);

        // Traverse leaf nodes and collect keys greater than minValue
        while (leaf != null) {
            if (leaf.getNodeType() == TreeNodeType.LeafNode) {
                BTreeLeafNode<TKey, TValue> leafNode = (BTreeLeafNode<TKey, TValue>) leaf;
                for (int i = 0; i < leafNode.getKeyCount(); i++) {
                    if (leafNode.getKey(i).compareTo(minValue) > 0) {
                        result.add(leafNode.getValue(i));
                    }
                }
            }
            leaf = leaf.getRightSibling();
        }
        return result;
    }
    
    
    public ArrayList<TValue> searchLessThan(TKey maxValue) {
        ArrayList<TValue> result = new ArrayList<>();
        BTreeNode<TKey> leaf = findLeafNodeShouldContainKey(maxValue);

        // Traverse leaf nodes towards the left and collect keys less than maxValue
        while (leaf != null) {
            if (leaf.getNodeType() == TreeNodeType.LeafNode) {
            	// get the leaf
                BTreeLeafNode<TKey, TValue> leafNode = (BTreeLeafNode<TKey, TValue>) leaf;
                // iterate over each key
                for (int i = 0; i < leafNode.getKeyCount(); i++) {
                    TKey currentKey = leafNode.getKey(i);
                    // compare and add
                    if (currentKey.compareTo(maxValue) < 0) {
                        // Add all values associated with the current key
                        result.add(leafNode.getValue(i));
                    }
                }
            }
            leaf = leaf.getLeftSibling(); // Traverse towards the left
        }
        return result;
    }
    
    public ArrayList<TValue> searchAllOccurrences(TKey key) {
    	
    	 BTreeNode<TKey> leaf = findLeafNodeShouldContainKey(key);
    	    while (leaf != null) {
    	        if (leaf.getNodeType() == TreeNodeType.LeafNode) {
    	            BTreeLeafNode<TKey, TValue> leafNode = (BTreeLeafNode<TKey, TValue>) leaf;
    	            int index = leafNode.search(key);
    	            if (index != -1) {
    	                Object value = leafNode.getValue(index);
    	                if (value instanceof ArrayList) {
    	                    // Found a vector associated with the key
    	                    return (ArrayList<TValue>) value;
    	                } else {
    	                    // Convert single value to a vector
    	                    ArrayList<TValue> valueVector = new ArrayList<>();
    	                    valueVector.add((TValue) value);
    	                    return valueVector;
    	                }
    	            }
    	        }
    	        leaf = leaf.getRightSibling();
    	    }
    	    return new ArrayList<>();
//    	ArrayList<TValue> occurrences = new ArrayList<>();
//    	BTreeNode<TKey> leaf = findLeafNodeShouldContainKey(key);
//        
//        // Traverse through the right siblings to collect all occurrences
//    	 while (leaf != null) {
//             if (leaf.getNodeType() == TreeNodeType.LeafNode) {
//             	// get the leaf
//                 BTreeLeafNode<TKey, TValue> leafNode = (BTreeLeafNode<TKey, TValue>) leaf;
//                 // iterate over each key
//                 for (int i = 0; i < leafNode.getKeyCount(); i++) {
//                     TKey currentKey = leafNode.getKey(i);
//                     // compare and add
//                     if (currentKey.compareTo(key) == 0) {
//                         // Add all values associated with the current key
//                         occurrences.add(leafNode.getValue(i));
//                     }
//                 }
//             }
//             // go to the right to find more duplicates. 
//             leaf = leaf.getRightSibling();
//         }
//        return occurrences;
    }
    
    
//    public void deleteAll() {
//        // Start from the root and traverse the tree to find all leaf nodes
//        deleteAllHelper(root);
//        // After deleting all leaf nodes, set the root to null to indicate an empty tree
//        root = null;
//    }

//    private void deleteAllHelper(BTreeNode<TKey> node) {
//        if (node == null) {
//            return;
//        }
//
//        // Recursively traverse all child nodes
//        if (!node.isLeaf()) {
//            BTreeInnerNode<TKey> innerNode = (BTreeInnerNode<TKey>) node;
//            for (int i = 0; i < innerNode.getChildrenCount(); i++) {
//                deleteAllHelper(innerNode.getChild(i));
//            }
//        } else {
//            // If it's a leaf node, delete it
//            BTreeLeafNode<TKey, TValue> leafNode = (BTreeLeafNode<TKey, TValue>) node;
//            leafNode.clear(); // Delete all key-value pairs in the leaf node
//        }
//    }
    
    public void clear() {
    	root = new BTreeLeafNode<>();
    }
}
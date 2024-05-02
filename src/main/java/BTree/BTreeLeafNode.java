package BTree;

import java.util.ArrayList;

import App.TupleReference;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author mohamed
 */
class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> implements Serializable{
	private final static long serialVersionUID = 82394820348204L;
	
	protected final static int LEAFORDER = 4;
	/**
	 * @uml.property name="values"
	 */
	private final Object[] values;
	
	/**
	 * @uml.property name="filters"
	 * @uml.associationEnd multiplicity="(0 -1)" elementType="java.lang.Boolean"
	 */
	private ArrayList<Boolean> filters;

	public BTreeLeafNode() {
		this.filters = new ArrayList<>();
		this.keys = new Object[LEAFORDER + 1];
		this.values = new Object[LEAFORDER + 1];
	}

	public BTreeLeafNode(BTreeLeafNode smallest) {
		this.keys = smallest.keys;
		this.values = smallest.values;
	}

	@SuppressWarnings("unchecked")
	public TValue getValue(int index) {
		return (TValue) this.values[index];
	}

	public void setValue(int index, TValue value) {
		this.values[index] = value;
	}

	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}

	@Override
	public int search(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			int cmp = this.getKey(i).compareTo(key);
			if (cmp == 0) {
				return i;
			} else if (cmp > 0) {
				return -1;
			}
		}

		return -1;
	}

	@Override
	public String toString() {
		String out = "";
		for (int index = 0; index < this.getKeyCount(); ++index) {
			out += this.getValue(index).toString() + " ";
		}
		return out;
	}

	/* The codes below are used to support insertion operation */

	public void insertKey(TKey key, TValue value) {
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0) {
			++index;
		}
		
		
		
		 if (index < this.getKeyCount() && this.getKey(index).compareTo(key) == 0) {
	            // Key already exists, handle duplicates by storing in a ArrayList
			 if (!(values[index] instanceof ArrayList)) {
		            // Convert existing value to ArrayList
		            ArrayList<TValue> valueList = new ArrayList<>();
		            valueList.add((TValue) values[index]); // Add existing value
		            values[index] = valueList;
		        }
	            
	           	
	            ArrayList<TValue> valueArrayList = (ArrayList <TValue>) values[index];
	            
	            if (valueArrayList.size() < 50) {
	                valueArrayList.add(value);
	            }
	            
		 }else {
			 
				this.insertAt(index, key, value);

		 }
	}

	private void insertAt(int index, TKey key, TValue value) {
		// move space for the new key
		for (int i = this.getKeyCount() - 1; i >= index; --i) {
			this.setKey(i + 1, this.getKey(i));
			this.setValue(i + 1, this.getValue(i));
		}

		// insert new key and value
		this.setKey(index, key);
		this.setValue(index, value);
		++this.keyCount;
	}

	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed
	 * to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
		int midIndex = this.getKeyCount() / 2;

		BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>();
		for (int i = midIndex; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex, this.getKey(i));
			newRNode.setValue(i - midIndex, this.getValue(i));
			this.setKey(i, null);
			this.setValue(i, null);
		}
		newRNode.keyCount = this.getKeyCount() - midIndex;
		this.keyCount = midIndex;

		return newRNode;
	}

	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		throw new UnsupportedOperationException();
	}

	/* The codes below are used to support deletion operation */

	public boolean delete(TKey key) {
		int index = this.search(key);
		if (index == -1)
			return false;

		this.deleteAt(index);
		return true;
	}

	public void deleteAt(int index) {
		int i = index;
		for (i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));
			this.setValue(i, this.getValue(i + 1));
		}
		this.setKey(i, null);
		this.setValue(i, null);
		--this.keyCount;
	}

	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Notice that the key sunk from parent is be abandoned.
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
		BTreeLeafNode<TKey, TValue> siblingLeaf = (BTreeLeafNode<TKey, TValue>) rightSibling;

		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
			this.setKey(j + i, siblingLeaf.getKey(i));
			this.setValue(j + i, siblingLeaf.getValue(i));
		}
		this.keyCount += siblingLeaf.getKeyCount();

		this.setRightSibling(siblingLeaf.rightSibling);
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.rightSibling.setLeftSibling(this);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
		BTreeLeafNode<TKey, TValue> siblingNode = (BTreeLeafNode<TKey, TValue>) sibling;

		this.insertKey(siblingNode.getKey(borrowIndex), siblingNode.getValue(borrowIndex));
		siblingNode.deleteAt(borrowIndex);

		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}

	@Override
	public String commit() {
		String result = "";
		for (int i = 0; i < values.length; i++) {
			if (values[i] == null) {
				break;
			}
			result += values[i].toString() + "\r\n";
		}
		return result;
	}

	@Override
	public BTreeLeafNode getSmallest() {
		return this;
	}	
}
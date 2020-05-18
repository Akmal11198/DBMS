import java.io.Serializable;
import java.util.ArrayList;


public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Ref[] records;
	private BPTreeLeafNode<T> next;
	
	@SuppressWarnings("unchecked")
	public BPTreeLeafNode(int n) 
	{
		super(n);
		keys = new Comparable[n];
		records = new Ref[n];

	}
	public boolean Contains(T key) {
		//System.out.println(keys[0]);
		for(int i = 0;i<this.keys.length-1;i++)
		{
			if(keys[i]!=null)
				if(this.keys[i].compareTo(key)==0)
					return true;
		}
		return false;				
						
	}
	
	/**
	 * @return the next leaf node
	 */
	public BPTreeLeafNode<T> getNext()
	{
		return this.next;
	}
	
	/**
	 * sets the next leaf node
	 * @param node the next leaf node
	 */
	public void setNext(BPTreeLeafNode<T> node)
	{
		this.next = node;
	}
	
	/**
	 * @param index the index to find its record
	 * @return the reference of the queried index
	 */
	public Ref getRecord(int index) 
	{
		return records[index];
	}
	
	/**
	 * sets the record at the given index with the passed reference
	 * @param index the index to set the value at
	 * @param recordReference the reference to the record
	 */
	public void setRecord(int index, Ref recordReference) 
	{
		records[index] = recordReference;
	}

	/**
	 * @return the reference of the last record
	 */
	public Ref getFirstRecord()
	{
		return records[0];
	}

	/**
	 * @return the reference of the last record
	 */
	public Ref getLastRecord()
	{
		return records[numberOfKeys-1];
	}
	
	/**
	 * finds the minimum number of keys the current node must hold
	 */
	public int minKeys()
	{
		if(this.isRoot())
			return 1;
		return (order + 1) / 2;
	}
	
	/**
	 * insert the specified key associated with a given record refernce in the B+ tree
	 */
	public PushUp<T> insert(T key, Ref recordReference, BPTreeInnerNode<T> parent, int ptr)
	{
		int index = 0;
		if(this.Contains(key))
		{
			while (getKey(index).compareTo(key) < 0)
				++index;
			records[index].duplicates.add(recordReference);
			return null;
		}	
		if(this.isFull())
		{
			BPTreeNode<T> newNode = this.split(key, recordReference);
			Comparable<T> newKey = newNode.getFirstKey();
			return new PushUp<T>(newNode, newKey);
		}
		
		
		else {	
		while (index < numberOfKeys && getKey(index).compareTo(key) <= 0)
			++index;
		this.insertAt(index, key, recordReference);
		return null;
		}
		
	}
	
	/**
	 * inserts the passed key associated with its record reference in the specified index
	 * @param index the index at which the key will be inserted
	 * @param key the key to be inserted
	 * @param recordReference the pointer to the record associated with the key
	 */
	private void insertAt(int index, Comparable<T> key, Ref recordReference) 
	{
		for (int i = numberOfKeys - 1; i >= index; --i) 
		{
			this.setKey(i + 1, getKey(i));
			this.setRecord(i + 1, getRecord(i));
		}

		this.setKey(index, key);
		this.setRecord(index, recordReference);
		++numberOfKeys;
	}
	
	/**
	 * splits the current node
	 * @param key the new key that caused the split
	 * @param recordReference the reference of the new key
	 * @return the new node that results from the split
	 */
	public BPTreeNode<T> split(T key, Ref recordReference) 
	{
		int keyIndex = this.findIndex(key);
		int midIndex = numberOfKeys / 2;
		if((numberOfKeys & 1) == 1 && keyIndex > midIndex)	//split nodes evenly
			++midIndex;		

		
		int totalKeys = numberOfKeys + 1;
		//move keys to a new node
		BPTreeLeafNode<T> newNode = new BPTreeLeafNode<T>(order);
		for (int i = midIndex; i < totalKeys - 1; ++i) 
		{
			newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
			numberOfKeys--;
		}
		
		//insert the new key
		if(keyIndex < totalKeys / 2)
			this.insertAt(keyIndex, key, recordReference);
		else
			newNode.insertAt(keyIndex - midIndex, key, recordReference);
		
		//set next pointers
		newNode.setNext(this.getNext());
		this.setNext(newNode);
		
		return newNode;
	}
	
	/**
	 * finds the index at which the passed key must be located 
	 * @param key the key to be checked for its location
	 * @return the expected index of the key
	 */
	public int findIndex(T key) 
	{
		for (int i = 0; i < numberOfKeys; ++i) 
		{
			int cmp = getKey(i).compareTo(key);
			if (cmp > 0) 
				return i;
		}
		return numberOfKeys;
	}

	/**
	 * returns the record reference with the passed key and null if does not exist
	 */
	@Override
	public ArrayList<Ref> search(T key) 
	{
		ArrayList<Ref> records = new ArrayList<>();
		for(int i = 0; i < numberOfKeys; ++i)
			if(getKey(i).compareTo(key) == 0) {
				getRecord(i);
				records.add(getRecord(i));
				//System.out.println(key + " " +getRecord(i).duplicates.size());
				for(Ref r : getRecord(i).duplicates)
					records.add(r);
			}
		return records;
	}
	
	/**
	 * delete the passed key from the B+ tree
	 */
	public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr,Ref ref) 
	{
		//boolean deleted = false;
		for(int i = 0; i < numberOfKeys; ++i)
			if(keys[i].compareTo(key) == 0)
			{
				Ref r = records[i];
				if(r.compareTo(ref)!=0) {
					System.out.println("deleted one of duplicates");
					for(int j = 0;j<r.duplicates.size();j++) {
						if(r.duplicates.get(j).compareTo(ref)==0) {
							r.duplicates.remove(j);
							return true;
						}
					}
				}
				if(r.duplicates.size()!=0) {
					System.out.println("Deleted Original Key");
					records[i] = r.duplicates.remove(0);
					return true;
				}
				
				
				deleteAt(i);
				if(i == 0 && ptr > 0)
				{
					//update key at parent
					parent.setKey(ptr - 1, this.getFirstKey());
				}
				//check that node has enough keys
				if(!this.isRoot() && numberOfKeys < this.minKeys())
				{
					//1.try to borrow
					if(borrow(parent, ptr))
						return true;
					//2.merge
					merge(parent, ptr);
				}
				return true;
			}
		return false;
	}
	
	/**
	 * delete a key at the specified index of the node
	 * @param index the index of the key to be deleted
	 */
	public void deleteAt(int index)
	{
		for(int i = index; i < numberOfKeys - 1; ++i)
		{
			keys[i] = keys[i+1];
			records[i] = records[i+1];
		}
		numberOfKeys--;
	}
	public String getPage(T key) 
	{
		ArrayList<Ref>refs=this.search(key);
		if(refs.size()>0) {
			return refs.get(refs.size()-1).getPageNo();
		}
		for(int i = 0; i < numberOfKeys; ++i)
			if(this.getKey(i).compareTo(key) > 0)
				return this.getRecord(i).getPageNo();
		return this.search((T) this.getKey(numberOfKeys-1)).get(this.search((T) this.getKey(numberOfKeys-1)).size()-1).getPageNo();
	}
	
	public void updateRef(T key, Ref recordReference,Ref oldr) 
	{
		for(int i=0;i<numberOfKeys;i++) {
			if(this.getKey(i).compareTo(key)==0) {
				System.out.println("key found"+" "+key);
				System.out.println(oldr.getPageNo()+" "+oldr.getIndexInPage());
				System.out.println(recordReference.getPageNo()+" "+recordReference.getIndexInPage());

				Ref r=this.getRecord(i);
				for(int j=r.duplicates.size()-1;j>-1;j--) {
					if(r.duplicates.get(j).getIndexInPage()==oldr.getIndexInPage()&&r.duplicates.get(j).getPageNo().contentEquals(oldr.getPageNo())) {
						r.duplicates.get(j).setPageNo(recordReference.getPageNo());
						r.duplicates.get(j).setIndexInPage(recordReference.getIndexInPage());
						System.out.println("found in duplicates");
						return;
					}	
				}
				if(r.getIndexInPage()==oldr.getIndexInPage()&&r.getPageNo().contentEquals(oldr.getPageNo())) {
					r.setPageNo(recordReference.getPageNo());
					r.setIndexInPage(recordReference.getIndexInPage());
					System.out.println("found in original");
					return;
				}
				System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
			}
		}
		System.out.println("key not found////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}	
	
	/**
	 * tries to borrow a key from the left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if borrow is done successfully and false otherwise
	 */
	public boolean borrow(BPTreeInnerNode<T> parent, int ptr)
	{
		//check left sibling
		if(ptr > 0)
		{
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
			if(leftSibling.numberOfKeys > leftSibling.minKeys())
			{
				this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());		
				leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
				parent.setKey(ptr - 1, keys[0]);
				return true;
			}
		}
		
		//check right sibling
		if(ptr < parent.numberOfKeys)
		{
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
			if(rightSibling.numberOfKeys > rightSibling.minKeys())
			{
				this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
				rightSibling.deleteAt(0);
				parent.setKey(ptr, rightSibling.getFirstKey());
				return true;
			}
		}
		return false;
	}
	
	/**
	 * merges the current node with its left or right sibling
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 */
	public void merge(BPTreeInnerNode<T> parent, int ptr)
	{
		if(ptr > 0)
		{
			//merge with left
			BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr-1);
			leftSibling.merge(this);
			parent.deleteAt(ptr-1);			
		}
		else
		{
			//merge with right
			BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr+1);
			this.merge(rightSibling);
			parent.deleteAt(ptr);
		}
	}
	
	
	/**
	 * merge the current node with the specified node. The foreign node will be deleted
	 * @param foreignNode the node to be merged with the current node
	 */
	public void merge(BPTreeLeafNode<T> foreignNode)
	{
		for(int i = 0; i < foreignNode.numberOfKeys; ++i)
			this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));
		
		this.setNext(foreignNode.getNext());
	}
}
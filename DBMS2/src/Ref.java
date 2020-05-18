import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Ref implements Serializable,Comparable{
	
	/**
	 * This class represents a pointer to the record. It is used at the leaves of the B+ tree 
	 */
	private static final long serialVersionUID = 1L;
	private String pageNo;
	private int indexInPage;
	Vector<Ref> duplicates; 
	
	public Ref(String pageNo, int indexInPage)
	{
		this.duplicates = new Vector<>();
		this.pageNo = pageNo;
		this.indexInPage = indexInPage;
	}
	
	public String getPageNo() {
		return pageNo;
	}

	public void setPageNo(String pageNo) {
		this.pageNo = pageNo;
	}

	public void setIndexInPage(int indexInPage) {
		this.indexInPage = indexInPage;
	}

	/**
	 * @return the page at which the record is saved on the hard disk
	 */
//	public String getPage()
//	{
//		return pageNo;
//	}
	
	/**
	 * @return the index at which the record is saved in the page
	 */
	public int getIndexInPage()
	{
		return indexInPage;
	}

	@Override
	public int compareTo(Object o) {
		Ref r = (Ref) o;
		// TODO Auto-generated method stub
		return (this.getPageNo().compareTo(r.getPageNo()))+
				(this.getIndexInPage() - r.getIndexInPage()) ;
	}
}
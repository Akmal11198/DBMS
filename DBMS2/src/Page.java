import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("serial")
public class Page implements Serializable{
	//transient String path;
	int size;
	Object min;
	@SuppressWarnings("rawtypes")
	Vector<Vector> rows;
	
	@SuppressWarnings("rawtypes")
	public Page(int size) {
		this.size=size;
	rows= new Vector<Vector>(size);	
	}
	
	public boolean isFull() {
		if(rows.size()<size)
			return false;
		return true;
	}
	
	
	public static void main(String[]args) {
		
	}

}

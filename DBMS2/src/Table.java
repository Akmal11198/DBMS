import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

@SuppressWarnings("serial")
public class Table implements Serializable {
	int nPages = 0;
	Vector<String> pageNames = new Vector<String>();
	transient Vector<Page> pages = new Vector<Page>();
	ArrayList<String>BPTNames=new ArrayList<String>();
	ArrayList<String>RTNames=new ArrayList<String>();
	transient ArrayList<String> ColNames=new ArrayList<String>();
	
	public Table() {
		
		
		
		}
	
}

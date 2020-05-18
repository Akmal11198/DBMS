import java.awt.Dimension;
import java.awt.Polygon;
import java.io.Serializable;
import java.util.Arrays;

@SuppressWarnings({ "rawtypes", "serial" })
public class DBpolygon extends Polygon implements Serializable,Comparable {
	Polygon p;
	private double Area;
	
	public double getArea() {
		return Area;
	}

	public DBpolygon(Polygon p) {
		Dimension dim = p.getBounds( ).getSize( );
		this.Area = dim.getHeight()*dim.getWidth();
		this.p = p;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		DBpolygon p = (DBpolygon)o;
		if((this.Area-p.Area)>0.0)
			return 1;
		if((this.Area-p.Area)==0.0)
			return 0;
		return -1;
	}
	
	
	public boolean equal(Polygon p2) {
		System.out.println(p);
		System.out.println(p2);
		Polygon p1 = this.p;
		
		System.out.println("hiiii");
		 if (p2 == null) {
			 System.out.println("null");
	          return false;
	      }
	      if (p2 == null) {
				 System.out.println("false 1");

	          return false;
	      }
	      if (p1.npoints != p2.npoints) {
				 System.out.println("false 2");

	          return false;
	      }
	      if (!Arrays.equals(p1.xpoints, p2.xpoints)) {
				 System.out.println("false 3");

	          return false;
	      }
	      if (!Arrays.equals(p1.ypoints, p2.ypoints)) {
				 System.out.println("false 4");

	          return false;
	      }
	      return true;
	}
	

}

package Nenenenene;

import java.awt.Dimension;
import java.awt.Polygon;

public class DBPolygon extends Polygon implements Comparable<DBPolygon> {
	Polygon poly;
	public DBPolygon(Polygon p) {
		poly = p;
	}

	@Override
	public int compareTo(DBPolygon p2) {
		Dimension dim = poly.getBounds().getSize( );
		Integer nThisArea = dim.width * dim.height;
		Dimension dim2 = p2.getBounds().getSize();
		Integer nOtherArea = dim2.width * dim2.height;
		return nThisArea.compareTo(nOtherArea);
	}

}

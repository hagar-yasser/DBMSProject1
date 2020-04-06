package Nenenenene;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

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
	
	public boolean equals(DBPolygon b){
		boolean equals = true;
		int [] x1 = this.poly.xpoints, x2 = b.poly.xpoints, y1 = this.poly.ypoints, y2 = this.poly.ypoints;
		for (int i = 0; i < x2.length; i++) {
			if(x1[i] != x2[i] || y1[i] != y2[i]){
				equals = false;
				break;
			}
		}
		return equals;
	}
}

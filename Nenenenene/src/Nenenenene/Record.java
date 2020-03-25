package Nenenenene;

import java.io.Serializable;
import java.util.Hashtable;

public class Record implements Serializable {
     Hashtable<String, Object> row;
     Position position;
     public Record(Hashtable<String,Object> h ,Position p) {
    	 row=h;
    	 position=p;
    	 
     }
}

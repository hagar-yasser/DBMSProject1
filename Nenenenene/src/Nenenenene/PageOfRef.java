package Nenenenene;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

public class PageOfRef implements Serializable {
	ArrayList<Ref> refs;
	int n;
	Properties config;
	String Treename, key;
	public int index;		//for printing the tree
	private static int nextIdx = 0;
	
	public void readProperty() throws DBAppException {
        config = new Properties();
        try {
            FileInputStream f = new FileInputStream("config/DBApp.properties");
            config.load(f);
        } catch (IOException e) {
            throw new DBAppException("Problem with reading the config file.");
        }

    }
	
	public PageOfRef(String Tree, String Key) throws DBAppException{
		readProperty();
        String s = config.getProperty("NodeSize");
        n = Integer.parseInt(s);
		Treename = Tree;
		key = Key;
		refs = new ArrayList<Ref>();
		index = nextIdx;
		nextIdx++;
	}

    
    public String getFilePath(){
    	String s = "data/" + Treename + "Ref" + key + "_" + index + ".class";
    	return s;
    }
	
	
			
	
}

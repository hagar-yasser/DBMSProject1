package Nenenenene;

import java.awt.*;
import java.io.*;
import java.util.*;

public class Page implements Serializable {
    Vector<Record> Rows;
    String key;
    String table;
    Properties config;
    int N;
    

    public Page(String keyy, String T) throws DBAppException {
        Rows = new Vector<Record>();
        key = keyy;
        table = T;
        readProperty();
        String s = config.getProperty("MaximumRowsCountinPage");
        N = Integer.parseInt(s);

    }

    public void readProperty() throws DBAppException {
        config = new Properties();
        try {
            FileInputStream f = new FileInputStream("config/DBApp.properties");
            config.load(f);
        } catch (IOException e) {
            throw new DBAppException("Problem with reading the config file.");
        }

    }

    public void insert(Hashtable<String, Object> row,int index) throws DBAppException {
        Object valueOfKey = row.get(key);
        if (Rows.size() == 0) {
            Rows.add(new Record(row, new Position(0,table+index)));
//			System.out.println("corner1");
            return;
        }
        if (Rows.size() == 1) {
            if (Compare(Rows.get(0).row.get(key), valueOfKey, key, table) > 0) {
                Rows.add(new Record(row, new Position(0,table+index)));
                Rows.get(1).position.i=1;
            } else {
                Rows.add(new Record(row, new Position(Rows.size(),table+index)));
            }
//			System.out.println("corner2");
            return;
        }
        int lo = 0;
        int hi = Rows.size() - 1;
        int mid = (hi + lo) / 2;
        while (lo < hi) {
            mid = (lo + hi) / 2;
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) > 0) {
                hi = mid;
            } else if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) < 0) {
                lo = mid + 1;
            } else {
                break;
            }


        }
        if (lo == hi) {
            mid = (lo + hi) / 2;
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) > 0) {
                hi = mid;
            } else if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) < 0) {
                lo = mid + 1;
            }
        }
        //System.out.println(mid);
        Rows.add(new Record(row, new Position(mid,table+index)));
        
        for(int j=mid+1;j<Rows.size();j++) {
        	Rows.get(j).position.i=Rows.get(j).position.i++;
        }

    }

    public int delete(Hashtable<String, Object> row ) throws DBAppException {
        Object valueOfKey = row.get(key);
        int deleted = 0;
        int lo = 0;
        int hi = Rows.size() - 1;
        int mid = (hi + lo) / 2;
        while (lo <= hi) {// gowa el while bn delete
            mid = (lo + hi) / 2;
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) > 0) {
                hi = mid - 1;
            }
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) < 0) {
                lo = mid + 1;
            }
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) == 0) {
                // awel mabala2y 7aga shabahy baroo7 lawel 7aga menha w a check
                // lw ha deleteha wala la2 w ashta3'al
                // System.out.println(mid);
                int i = mid;
                for (; i >= 0; i--) {
                    if (Compare(Rows.get(i).row.get(key), valueOfKey, key, table) != 0) {
                        break;
                    }
                }
                i++;
                if (Compare(Rows.get(i).row.get(key), valueOfKey, key, table) != 0) {
                    // sometimes index is before the one I shall start from
                    i++;
                }
                // System.out.println(i);
                int j = Math.max(i, 0);
                int size = Rows.size();
                for (; j < size; j++) {
                    // System.out.println("hey "+i);
                    if (Compare(Rows.get(i).row.get(key), valueOfKey, key, table) != 0) {
                        break;
                    }
                    Set<String> keys = row.keySet();
                    boolean identical = true;
                    for (String k : keys) {
                        // System.out.println(k);
                        // System.out.println(((Comparable)(Rows.get(i).get(k))));
                        if (Compare(Rows.get(i).row.get(k), row.get(k), k, table) != 0) {
                            identical = false;
                            i++;
                            break;
                        }
                    }
                    if (identical) {
                        // System.out.println("lala");
                        Rows.remove(i);
                        int v=Rows.size();
                        for(int k=v;k>=i;k-- ) {
                        	Rows.get(k).position.i=Rows.get(k).position.i--;
                        }
                        deleted++;
                    }
                }
                break;
            }
        }
		if(deleted==0)
		    throw new DBAppException("This record does not exist");
        return deleted;
    }

    public int linearDelete(Hashtable<String, Object> row) throws DBAppException {
        int countDeleted = 0;
        Set<String> keys = row.keySet();
        int size = Rows.size() - 1;
        for (int i = size; i >= 0; i--) {
            boolean identical = true;
            for (String k : keys) {
                if (Compare(Rows.get(i).row.get(k), row.get(k), k, table) != 0) {
                    identical = false;
                    break;
                }
            }
            if (identical) {
                countDeleted++;
                Rows.remove(i);
                int v=Rows.size();
                for(int j=v;j>=i;j--) {  //SHIFTING ALL RECORDS IN A PAGE AFTER A DELETE IS DONE
                	Rows.get(j).position.i=Rows.get(j).position.i--;
                }
            }
        }
        return countDeleted;
    }

    public int update(Hashtable<String, Object> row) throws DBAppException {
        Object valueOfKey = row.get(key);
        int lo = 0;
        int updated = 0;
        int hi = Rows.size() - 1;
        int mid = (hi + lo) / 2;
        while (lo <= hi) {// gowa el while bn update
            mid = (lo + hi) / 2;
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) > 0) {
                hi = mid - 1;
            }
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) < 0) {
                lo = mid + 1;
            }
//			System.out.println(Rows.get(mid).get(key));
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) == 0) {
                int i = mid;
                for (; i >= 0; i--) { //ADDED AN EQUAL
                    if (Compare(Rows.get(i).row.get(key), valueOfKey, key, table) != 0) {
                        i++;
                        break;
                    }
                }
                if (Compare(Rows.get(Math.max(i, 0)).row.get(key), valueOfKey, key, table) != 0) {
                    // sometimes index is before the one I shall start from
                    i++;
                }
                int j = Math.max(i, 0);
                int size = Rows.size();
//				System.out.println("llll");
                for (; j < size; j++) {
                    if (Compare(Rows.get(j).row.get(key), valueOfKey, key, table) != 0) {
                        break;
                    }
//					System.out.println("reached" + j);
                    Set<String> keys = row.keySet();
                    for (String k : keys) {
//						System.out.println("updating");
                        Rows.get(j).row.remove(k);
                        Rows.get(j).row.put(k, row.get(k));
                    }
                    updated++;
                }
                break;
            }
        }
        return updated;
    }

    public int Compare(Object x, Object y, String ColumnName, String table) throws DBAppException {
        BufferedReader br;
        String s = "";
        try {
            br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("can not read metadata file");
        } catch (IOException IO) {
            throw new DBAppException("failed to read metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(table) || !st[1].equals(ColumnName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        return ((Integer) x).compareTo((Integer) y);
                    case "java.lang.String":
                        return (x.toString()).compareTo(y.toString());
                    case "java.lang.Double":
                        return ((Double) x).compareTo((Double) y);
                    case "java.lang.Boolean":
                        return ((Boolean) x).compareTo((Boolean) y);
                    case "java.util.Date":
                        return ((Date) x).compareTo((Date) y);
                    case "java.awt.Polygon":
                        DBPolygon PX = new DBPolygon((Polygon) x), PY = new DBPolygon((Polygon) y);
                        return (PX).compareTo(PY);
                }
            }
        } catch (IOException e) {
            throw new DBAppException("failed to read metadata file");
        }
        return 0;
    }

}

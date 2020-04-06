package Nenenenene;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

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

    public int insert(Hashtable<String, Object> row,int index,ArrayList<BPTree> BPTrees) throws DBAppException {
        Object valueOfKey = row.get(key);
        if (Rows.size() == 0) {
            Rows.add(new Record(row, new Position(0,table+index)));
            Ref r=new Ref(table+index, 0);
            Set<String> keys = row.keySet();
            for (String k : keys) {
            	for(int j=0;j<BPTrees.size();j++) {
            		if(BPTrees.get(j).ColName.equals(k)) {
            			
            			int type=getType(k);
            			if(type==1)
            				BPTrees.get(j).insert((Integer)row.get(k), r);
            			if(type==2)
                			BPTrees.get(j).insert((String)row.get(k), r);
            			if(type==3)
                			BPTrees.get(j).insert((double)row.get(k), r);
            			if(type==4)
                			BPTrees.get(j).insert((boolean)row.get(k), r);
            			if(type==5)
                			BPTrees.get(j).insert((Date)row.get(k), r);
            			
            			
            			//BPTrees.get(j).insert((Comparable)row.get(k), r);
            		}
            	}
            }
            

            return 0;
        }
        if (Rows.size() == 1) {
            if (Compare(Rows.get(0).row.get(key), valueOfKey, key, table) > 0) {
                Rows.add(new Record(row, new Position(0,table+index)));
                Ref r=new Ref(table+index, 0);
                Set<String> keys = row.keySet();
                for (String k : keys) {
                	for(int j=0;j<BPTrees.size();j++) {
                		if(BPTrees.get(j).ColName.equals(k)) {
                			
                			int type=getType(k);
                			if(type==1)
                			BPTrees.get(j).insert((Integer)row.get(k), r);
                			if(type==2)
                    			BPTrees.get(j).insert((String)row.get(k), r);
                			if(type==3)
                    			BPTrees.get(j).insert((double)row.get(k), r);
                			if(type==4)
                    			BPTrees.get(j).insert((boolean)row.get(k), r);
                			if(type==5)
                    			BPTrees.get(j).insert((Date)row.get(k), r);
                			
                	      		
                			//BPTrees.get(j).insert((Comparable)row.get(k), r);
                		}
                	}
                }
                Rows.get(1).position.i=1;
                Ref old=new Ref(table+index, 0);
                Ref neww=new Ref(table+index, 1);
                for (String k : keys) {
                	for(int j=0;j<BPTrees.size();j++) {
                		if(BPTrees.get(j).ColName.equals(k)) {
                			
                			
                			int type=getType(k);
                			if(type==1)
                			BPTrees.get(j).updateRef((Integer)Rows.get(1).row.get(k),old,neww);
                			if(type==2)
                    			BPTrees.get(j).updateRef((String)Rows.get(1).row.get(k),old,neww);
                			if(type==3)
                    			BPTrees.get(j).updateRef((double)Rows.get(1).row.get(k),old,neww);
                			if(type==4)
                    			BPTrees.get(j).updateRef((boolean)Rows.get(1).row.get(k),old,neww);
                			if(type==5)
                    			BPTrees.get(j).updateRef((Date)Rows.get(1).row.get(k),old,neww);
                			
                			//BPTrees.get(j).updateRef((Comparable)Rows.get(1).row.get(k),old,neww);
                		}
                	}
                }
                
                
                
                
                
                return 0;
            } else {
                Rows.add(new Record(row, new Position(Rows.size(),table+index)));
                Ref r=new Ref(table+index, Rows.size());
                Set<String> keys = row.keySet();
                for (String k : keys) {
                	for(int j=0;j<BPTrees.size();j++) {
                		if(BPTrees.get(j).ColName.equals(k)) {
                			
                			int type=getType(k);
                			if(type==1)
                			BPTrees.get(j).insert((Integer)row.get(k), r);
                			if(type==2)
                    			BPTrees.get(j).insert((String)row.get(k), r);
                			if(type==3)
                    			BPTrees.get(j).insert((double)row.get(k), r);
                			if(type==4)
                    			BPTrees.get(j).insert((boolean)row.get(k), r);
                			if(type==5)
                    			BPTrees.get(j).insert((Date)row.get(k), r);
                			
                			
                			//BPTrees.get(j).insert((Comparable)row.get(k), r);
                		}
                	}
                }
               
                
            }

            return 1;  
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
        Ref r=new Ref(table+index, mid);
        Set<String> keys = row.keySet();
        for (String k : keys) {
        	for(int j=0;j<BPTrees.size();j++) {
        		if(BPTrees.get(j).ColName.equals(k)) {
        			int type=getType(k);
        			if(type==1)
        			BPTrees.get(j).insert((Integer)row.get(k), r);
        			if(type==2)
            			BPTrees.get(j).insert((String)row.get(k), r);
        			if(type==3)
            			BPTrees.get(j).insert((double)row.get(k), r);
        			if(type==4)
            			BPTrees.get(j).insert((boolean)row.get(k), r);
        			if(type==5)
            			BPTrees.get(j).insert((Date)row.get(k), r);
        			
        			//BPTrees.get(j).insert((Comparable)row.get(k), r);
        		}
        	}
        }
        
              
        
        for(int j=mid+1;j<Rows.size();j++) {
        	 Ref old=new Ref(table+index,Rows.get(j).position.i );
        	Rows.get(j).position.i=Rows.get(j).position.i++;
            Ref neww=new Ref(table+index, Rows.get(j).position.i);
            for (String k : keys) {
            	for(int m=0;m<BPTrees.size();m++) {
            		if(BPTrees.get(m).ColName.equals(k)) {
            			
            			int type=getType(k);
            			if(type==1)
            			BPTrees.get(m).updateRef((Integer)Rows.get(j).row.get(k),old,neww);
            			if(type==2)
                			BPTrees.get(m).updateRef((String)Rows.get(j).row.get(k),old,neww);
            			if(type==3)
                			BPTrees.get(m).updateRef((double)Rows.get(j).row.get(k),old,neww);
            			if(type==4)
                			BPTrees.get(m).updateRef((boolean)Rows.get(j).row.get(k),old,neww);
            			if(type==5)
                			BPTrees.get(m).updateRef((Date)Rows.get(j).row.get(k),old,neww);
            			
            			//BPTrees.get(m).updateRef((Comparable)Rows.get(j).row.get(k),old,neww);
            		}
            	}
            }
        }
               return mid;
    }
    public void updateAllIndexesAfterDelete(Hashtable<String, Object> row,int index,ArrayList<BPTree> BPTrees) throws DBAppException {
    	for(int i=0;i<Rows.size();i++) {
    		Ref old =new Ref(table+index,Rows.get(i).position.i);
    		Rows.get(i).position.i=i;
    		Ref neww=new Ref(table+index, Rows.get(i).position.i);
    		Set<String> keys = row.keySet();
    		for (String k : keys) {
            	for(int j=0;j<BPTrees.size();j++) {
            		if(BPTrees.get(j).ColName.equals(k)) {
            			
            			int type=getType(k);
            			if(type==1)
            			BPTrees.get(j).updateRef((Integer)row.get(k),old,neww);
            			if(type==2)
                			BPTrees.get(j).updateRef((String)row.get(k),old,neww);
            			if(type==3)
                			BPTrees.get(j).updateRef((double)row.get(k),old,neww);
            			if(type==4)
                			BPTrees.get(j).updateRef((boolean)row.get(k),old,neww);
            			if(type==5)
                			BPTrees.get(j).updateRef((Date)row.get(k),old,neww);
    		
    		
    	}
       }
    }
    	}
    	
    }
    public void deleteAt(Record record,ArrayList<BPTree> BPTrees) throws DBAppException {
        Ref r=new Ref(record.position.pagename, record.position.i);
        for (String k : record.row.keySet()) {
            for(int m=0;m<BPTrees.size();m++) {
                if(BPTrees.get(m).ColName.equals(k)) {
                    int type=getType(k);
                    if(type==1)
                        BPTrees.get(m).deleteSingleRef((Integer)record.row.get(k), r);
                    if(type==2)
                        BPTrees.get(m).deleteSingleRef((String)record.row.get(k), r);
                    if(type==3)
                        BPTrees.get(m).deleteSingleRef((double)record.row.get(k), r);
                    if(type==4)
                        BPTrees.get(m).deleteSingleRef((boolean)record.row.get(k), r);
                    if(type==5)
                        BPTrees.get(m).deleteSingleRef((Date)record.row.get(k), r);


                    //BPTrees.get(m).deleteSingleRef((Comparable)row.get(k), r);
                }
            }
        }
        Rows.remove(record.position.i);
        int indx=0;
        for (int j = 0; j <record.position.pagename.length() ; j++) {
            if(record.position.pagename.charAt(j)>='0'&&record.position.pagename.charAt(j)<='9') {
                indx = j;
                break;
            }
        }
        indx=Integer.parseInt(record.position.pagename.substring(indx));
        updateAllIndexesAfterDelete(record.row, indx, BPTrees);

    }

    public int delete(Hashtable<String, Object> row,int index,ArrayList<BPTree> BPTrees ) throws DBAppException {
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
                    if (Compare(Rows.get(j).row.get(key), valueOfKey, key, table) != 0) {
                        break;
                    }
                    Set<String> keys = row.keySet();
                    boolean identical = true;
                    for (String k : keys) {
                    	String typeOfKey = getType(table,k);
                        if(typeOfKey.equals("java.awt.Polygon")){
                        	if(!Rows.get(j).row.get(k).equals(row.get(k))){
                        		identical = false;
                                j++;
                                break;
                        	}
                        }
                        // System.out.println(k);
                        // System.out.println(((Comparable)(Rows.get(i).get(k))));
                        if (Compare(Rows.get(j).row.get(k), row.get(k), k, table) != 0) {
                            identical = false;
                            j++;
                            break;
                        }
                    }
                    if (identical) {
                        // System.out.println("lala");
                    	Ref r=new Ref(table+index, j);
                        for (String k : keys) {
                        	for(int m=0;m<BPTrees.size();m++) {
                        		if(BPTrees.get(m).ColName.equals(k)) {
                        			int type=getType(k);
                        			if(type==1)
                        			BPTrees.get(m).deleteSingleRef((Integer)row.get(k), r);
                        			if(type==2)
                            			BPTrees.get(m).deleteSingleRef((String)row.get(k), r);
                        			if(type==3)
                            			BPTrees.get(m).deleteSingleRef((double)row.get(k), r);
                        			if(type==4)
                            			BPTrees.get(m).deleteSingleRef((boolean)row.get(k), r);
                        			if(type==5)
                            			BPTrees.get(m).deleteSingleRef((Date)row.get(k), r);
                        			
                        			
                        			//BPTrees.get(m).deleteSingleRef((Comparable)row.get(k), r);
                        		}
                        	}
                        }
                        Rows.remove(j);
                       
//                        int v=Rows.size();
//                        for(int k=v;k>=i;k-- ) {
//                        	Rows.get(k).position.i=Rows.get(k).position.i--;
//                        	Ref old=new Ref(table+index, Rows.get(k).position.i);
//                        	Ref neww=new Ref(table+index, Rows.get(k).position.i--);
//                            for (String h : keys) {
//                            	for(int m=0;m<BPTrees.size();m++) {
//                            		if(BPTrees.get(m).ColName.equals(h)) {
//                            			
//                            			int type=getType(h);
//                            			if(type==1)
//                            			BPTrees.get(m).updateRef((Integer)row.get(h),old,neww);
//                            			if(type==2)
//                                			BPTrees.get(m).updateRef((String)row.get(h),old,neww);
//                            			if(type==3)
//                                			BPTrees.get(m).updateRef((double)row.get(h),old,neww);
//                            			if(type==4)
//                                			BPTrees.get(m).updateRef((boolean)row.get(h),old,neww);
//                            			if(type==5)
//                                			BPTrees.get(m).updateRef((Date)row.get(h),old,neww);
//                            			
//                            		//	BPTrees.get(m).updateRef((Comparable)row.get(h),old,neww);
//                            		}
//                            	}
//                            }
//                        }
                        deleted++;
                    }
                }
                updateAllIndexesAfterDelete(row, index, BPTrees);
                break;
            }
        }
		if(deleted==0)
		    throw new DBAppException("This record does not exist");
        return deleted;
    }


    public int linearDelete(Hashtable<String, Object> row, ArrayList<BPTree> BPTrees,int index) throws DBAppException {
        int countDeleted = 0;
        Set<String> keys = row.keySet();
        int size = Rows.size() - 1;
        for (int i = size; i >= 0; i--) {
            boolean identical = true;
            for (String k : keys) {
            	String typeOfKey = getType(table,k);
                if(typeOfKey.equals("java.awt.Polygon")){
                	if(!Rows.get(i).row.get(k).equals(row.get(k))){
                		identical = false;
                        break;
                	}
                }
                if (Compare(Rows.get(i).row.get(k), row.get(k), k, table) != 0) {
                    identical = false;
                    break;
                }
            }
            if (identical) {
                countDeleted++;
                Ref r=new Ref(table+index,i);
              
                for (String k : keys) {
                	for(int j=0;j<BPTrees.size();j++) {
                		if(BPTrees.get(j).ColName.equals(k)) {
                			int type=getType(k);
                			if(type==1)
                			BPTrees.get(j).deleteSingleRef((Integer)row.get(k), r);
                			if(type==2)
                    			BPTrees.get(j).deleteSingleRef((String)row.get(k), r);
                			if(type==3)
                    			BPTrees.get(j).deleteSingleRef((double)row.get(k), r);
                			if(type==4)
                    			BPTrees.get(j).deleteSingleRef((boolean)row.get(k), r);
                			if(type==5)
                    			BPTrees.get(j).deleteSingleRef((Date)row.get(k), r);
                			
                			
                		}
                	}
                }
                
                Rows.remove(i);
//                int v=Rows.size();
//                for(int j=v;j>=i;j--) {  //SHIFTING ALL RECORDS IN A PAGE AFTER A DELETE IS DONE
//                	Rows.get(j).position.i=Rows.get(j).position.i--;
//                	Ref old=new Ref(table+index, Rows.get(j).position.i);
//                	Ref neww=new Ref(table+index, Rows.get(j).position.i--);
//                	
//                	for (String k : keys) {
//                    	for(int m=0;m<BPTrees.size();m++) {
//                    		if(BPTrees.get(m).ColName.equals(k)) {
//                    			int type=getType(k);
//                    			if(type==1)
//                    				BPTrees.get(m).updateRef((Integer)Rows.get(j).row.get(k), old, neww);
//                    			if(type==2)
//                        			BPTrees.get(m).updateRef((String)Rows.get(j).row.get(k), old, neww);
//                    			if(type==3)
//                        			BPTrees.get(m).updateRef((double)Rows.get(j).row.get(k), old, neww);
//                    			if(type==4)
//                        			BPTrees.get(m).updateRef((boolean)Rows.get(j).row.get(k), old, neww);
//                    			if(type==5)
//                        			BPTrees.get(m).updateRef((Date)Rows.get(j).row.get(k), old, neww);
//                    			
//                    			
                    			
                    		}
                    	}
        updateAllIndexesAfterDelete(row, index, BPTrees);
                    
                	
      
        return countDeleted;
    }

    public int update(Hashtable<String, Object> row,int index,ArrayList<BPTree> BPTrees) throws DBAppException {
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
                    String typeOfKey = getType(table,key);
                    if(typeOfKey.equals("java.awt.Polygon")){
                    	if(!Rows.get(j).row.get(key).equals(valueOfKey)){
                    		continue;
                    	}
                    }
//					System.out.println("reached" + j);
                    Set<String> keys = row.keySet();
                    for (String k : keys) {
//						System.out.println("updating");
                        
                       Ref r=new Ref(table+index, j);
                        	for(int m=0;m<BPTrees.size();m++) {
                        		if(BPTrees.get(m).ColName.equals(k)) {
                        			int type=getType(k);
                        			if(type==1)
                        			BPTrees.get(m).deleteSingleRef((Integer)Rows.get(j).row.get(k), r);
                        			if(type==2)
                            			BPTrees.get(m).deleteSingleRef((String)Rows.get(j).row.get(k), r);
                        			if(type==3)
                            			BPTrees.get(m).deleteSingleRef((double)Rows.get(j).row.get(k),r);
                        			if(type==4)
                            			BPTrees.get(m).deleteSingleRef((boolean)Rows.get(j).row.get(k), r);
                        			if(type==5)
                            			BPTrees.get(m).deleteSingleRef((Date)Rows.get(j).row.get(k),r);
                        				
                        			
                        			//BPTrees.get(m).deleteSingleRef((Comparable)Rows.get(j).row.get(k),r );
                        		}
                        	}
                        
                    Rows.get(j).row.remove(k);
                        Rows.get(j).row.put(k, row.get(k));
                         r=new Ref(table+index, j);
                    	for(int m=0;m<BPTrees.size();m++) {
                    		if(BPTrees.get(m).ColName.equals(k)) {
                    			
                    			int type=getType(k);
                    			if(type==1)
                    			BPTrees.get(m).insert((Integer)Rows.get(j).row.get(k), r);
                    			if(type==2)
                        			BPTrees.get(m).insert((String)Rows.get(j).row.get(k), r);
                    			if(type==3)
                        			BPTrees.get(m).insert((double)Rows.get(j).row.get(k),r);
                    			if(type==4)
                        			BPTrees.get(m).insert((boolean)Rows.get(j).row.get(k), r);
                    			if(type==5)
                        			BPTrees.get(m).insert((Date)Rows.get(j).row.get(k),r);
                    			
                    			
                    			//BPTrees.get(m).insert((Comparable)Rows.get(j).row.get(k),r );
                    		}
                    	}
                        
                    }
                    }
                    updated++;
                }
                break;
            
        }
        return updated;
    }
    public void updateAt(Record record,ArrayList<BPTree> BPTrees) throws DBAppException {
        Hashtable<String,Object>row=record.row;
        int j=record.position.i;
        Set<String> keys = row.keySet();
        for (String k : keys) {
//						System.out.println("updating");

            Ref r=new Ref(record.position.pagename, j);
            for(int m=0;m<BPTrees.size();m++) {
                if(BPTrees.get(m).ColName.equals(k)) {
                    int type=getType(k);
                    if(type==1)
                        BPTrees.get(m).deleteSingleRef((Integer)Rows.get(j).row.get(k), r);
                    if(type==2)
                        BPTrees.get(m).deleteSingleRef((String)Rows.get(j).row.get(k), r);
                    if(type==3)
                        BPTrees.get(m).deleteSingleRef((double)Rows.get(j).row.get(k),r);
                    if(type==4)
                        BPTrees.get(m).deleteSingleRef((boolean)Rows.get(j).row.get(k), r);
                    if(type==5)
                        BPTrees.get(m).deleteSingleRef((Date)Rows.get(j).row.get(k),r);


                    //BPTrees.get(m).deleteSingleRef((Comparable)Rows.get(j).row.get(k),r );
                }
            }

            Rows.get(j).row.remove(k);
            Rows.get(j).row.put(k, row.get(k));
            r=new Ref(record.position.pagename, j);
            for(int m=0;m<BPTrees.size();m++) {
                if(BPTrees.get(m).ColName.equals(k)) {

                    int type=getType(k);
                    if(type==1)
                        BPTrees.get(m).insert((Integer)Rows.get(j).row.get(k), r);
                    if(type==2)
                        BPTrees.get(m).insert((String)Rows.get(j).row.get(k), r);
                    if(type==3)
                        BPTrees.get(m).insert((double)Rows.get(j).row.get(k),r);
                    if(type==4)
                        BPTrees.get(m).insert((boolean)Rows.get(j).row.get(k), r);
                    if(type==5)
                        BPTrees.get(m).insert((Date)Rows.get(j).row.get(k),r);


                    //BPTrees.get(m).insert((Comparable)Rows.get(j).row.get(k),r );
                }
            }

        }
    }
    
    public String getType(String table, String keyCol) throws DBAppException{
    	String ans = "";
    	BufferedReader br;
        String s = "";
        try {
            br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("Can not read metadata file");
        } catch (IOException IO) {
            throw new DBAppException("Failed to read metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(table) || !st[1].equals(keyCol)) continue;
                String value = st[2];
                return value;
            }
        } catch (IOException e) {
            throw new DBAppException("Failed to read metadata file");
        }
		return ans;
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
    
    
    
    
    
    public int getType(String ColName) throws DBAppException {
        BufferedReader br;
        String s = "";
        try {
            br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("can not find metadata file");
        } catch (IOException IO) {
            throw new DBAppException("can not write to metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(table) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        return 1;
                    case "java.lang.String":
                        return 2;
                    case "java.lang.Double":
                        return 3;
                    case "java.lang.Boolean":
                        return 4;
                    case "java.util.Date":
                        return 5;
                    
                }
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
        return -1;
    }

}

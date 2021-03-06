package Nenenenene;

import java.awt.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;

public class Table implements Serializable {
    int pages;
    String Key;
    String Name;
    ArrayList<String> PagesNames;
    ArrayList<BPTree> BPTrees;

    public Table(String key, String name) {
        pages = 0;
        Key = key;
        Name = name;
        PagesNames=new ArrayList<>();
        BPTrees=new ArrayList<>();
    }

    public Page deserialize(String name, int index) throws DBAppException {
        Page current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/" + name + index + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No file with this name");
        }
        return current;
    }
    public Page deserialize(String name) throws DBAppException {
        Page current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/" + name  + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No file with this name");
        }
        return current;
    }

    public void serialize(Page p, String name, int index) throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/" + name + index + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            throw new DBAppException("Can not serialize Page");
        }
    }
    public void serialize(Page p, String name) throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/" + name + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            throw new DBAppException("Can not serialize Page");
        }
    }

    public void linearDeleteFromPage(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        for (int i = 0; i < pages; i++) {
            int deleted = 1;
            while (deleted != 0) {
                Page current = deserialize(Name, i);
                deleted = current.linearDelete(HtblColNameValue);

                if (deleted != 0 && current.Rows.size() == 0) {//CONDITION FOR LAZY SHIFTING
                    serialize(current, Name, i);
                    shift(i, current.N);//SHIFT IS CALLED WITH current.N BECAUSE NOW PAGE IS EMPTY
                } else
                    serialize(current, Name, i);
            }
        }

    }


    private static void deleteFile(String filename) {
        File f = new File(filename);
        f.delete();

    }

    public void shift(int index, int n) throws DBAppException {
        if (index < pages - 1) {
            int size = pages;
            for (int i = index + 1; i <= size - 1; i++) {
//				Page old = deserialize(Name, index);
                Page neww = deserialize(Name, i);
                serialize(neww, Name, i - 1);
            }
        }
        deleteFile("data/" + Name + (pages - 1) + ".class");
        pages--;

    }

    public void insertIntoPage(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        Object valueOfKey = HtblColNameValue.get(Key);
        //CORNER CASE IF EMPTY TABLE
        if (pages == 0) {
            Page last = new Page(Key, Name);
            last.Rows.add(new Record(HtblColNameValue, new Position(HtblColNameValue.size(), Name + "0")));
            pages = 1;
            serialize(last, Name, 0);
            return;
        }
        //CORNER CASES IF BIGGEST OR SMALLEST
        int mid = 0;
        Page current = deserialize(Name, 0);
        if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {  //in this case i did not handle shifting if page is already full rokaya
            current.Rows.add(new Record(HtblColNameValue, new Position(0, Name + "0")));
            int v = current.Rows.size() - 1;
            for (int i = v; i > 0; i--) { //shifting all records after i insert
                current.Rows.get(i).position.i = current.Rows.get(i).position.i++;
            }

            serialize(current, Name, 0);
        } else {
            serialize(current, Name, 0);
            current = deserialize(Name, pages - 1);
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                if (current.Rows.size() < current.N) {
                    current.Rows.add(new Record(HtblColNameValue, new Position(HtblColNameValue.size(), Name + "" + (pages - 1))));
                    serialize(current, Name, pages - 1);
                } else {//BIGGER THAN THE BIGGEST WHICH IS IN THE LAST ROW IN THE LAST PAGE SO WE CREATE A NEW PAGE
                    serialize(current, Name, pages - 1);
                    Page last = new Page(Key, Name);
                    last.Rows.add(new Record(HtblColNameValue, new Position(HtblColNameValue.size(), Name + "" + pages)));
                    pages++;
                    serialize(last, Name, pages - 1);
                }

                return;
            }
            serialize(current, Name, pages - 1);//added line
            int lo = 0;
            int hi = pages - 1;
            mid = (lo + hi) / 2;
            while (lo <= hi) {
                mid = (lo + hi) / 2;
                current = deserialize(Name, mid);
                if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {//kda we will have many pages deserialised rokaya
                    lo = mid + 1;
                    continue;
                }
                if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {
                    hi = mid - 1;
                } else {
                    serialize(current, Name, mid);//added line
                    break;
                }
                serialize(current, Name, mid);

            }
            current = deserialize(Name, mid);//added line
            current.insert(HtblColNameValue, mid);
            serialize(current, Name, mid);

        }

        //SHIFTING PHASE
        Page now = deserialize(Name, mid);
        if (now.Rows.size() > now.N) {//CONDITION FOR LAZY SHIFTING
            Hashtable<String, Object> h = (now.Rows.remove(now.Rows.size() - 1)).row;//remove badal add
            serialize(now, Name, mid);//added line
            for (int i = mid + 1; i < pages; i++) {//page.size()not-1
                Page after = deserialize(Name, i);
                after.Rows.add(new Record(h, new Position(0, Name + i)));
                int v = after.Rows.size();
                for (int j = v - 1; j > 0; j--) {
                    after.Rows.get(j).position.i = after.Rows.get(j).position.i++;
                }
                if (after.Rows.size() <= after.N) {//CONDITION FOR LAZY SHIFTING
                    serialize(after, Name, i);
                    return;
                }
                h = (after.Rows.remove(after.Rows.size() - 1)).row;
                serialize(after, Name, i);

            }
            current = deserialize(Name, pages - 1);
            if (current.Rows.size() < current.N) {
                current.Rows.add(new Record(h, new Position(h.size(), Name + (pages - 1))));
                serialize(current, Name, pages - 1);
                return;
            } else {//creating new page
                serialize(current, Name, pages - 1);
                Page last = new Page(Key, Name);
                last.Rows.add(new Record(h, new Position(0, Name + pages)));
                serialize(last, Name, pages);
                pages++;
            }
        }
    }

    public void deleteFromPage(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        if (!HtblColNameValue.containsKey(Key)) {
            linearDeleteFromPage(HtblColNameValue);
            return;
        }
        Object valueOfKey = HtblColNameValue.get(Key);
        int lo = 0;
        int hi = pages - 1;
        // System.out.println(pages-1);
        int mid = (lo + hi) / 2;
        boolean found = false;
        while (lo <= hi && !found) {
            mid = (lo + hi) / 2;
            Page current = deserialize(Name, mid);
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                lo = mid + 1;
                serialize(current, Name, mid);
                continue;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {
                hi = mid - 1;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) <= 0
                    && Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) >= 0) {
                found = true;
            }
            serialize(current, Name, mid);
        }
        if (!found)
            throw new DBAppException("This record does not exist");
        int i = mid;
        for (; i >= 0; i--) {
            Page current = deserialize(Name, i);
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                serialize(current, Name, i);
                //i++;
                break;
            }
            serialize(current, Name, i);
        }
        i++;
        int size = pages;
        for (int j = i; j < size; j++) {
            Page current = null;
            int deleted = 1;
            boolean flag = false;
            while (deleted != 0 && j < pages) {
                current = deserialize(Name, j);
                flag = false;
                deleted = current.delete(HtblColNameValue);
                if (deleted != 0) {
                    flag = true;
                    if (current.Rows.size() == 0) {//CONDITION FOR LAZY SHIFTING
                        serialize(current, Name, j);
                        shift(j, current.N);//SHIFT IS CALLED WITH current.N BECAUSE NOW PAGE IS EMPTY
                    } else
                        serialize(current, Name, j);
                }
            }
            if (!flag)
                serialize(current, Name, j);
        }


    }

    public void updatePage(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        Object valueOfKey = HtblColNameValue.get(Key);
        int lo = 0;
        int hi = pages - 1;
        int mid = (lo + hi) / 2;
        boolean found = false;
        while (lo <= hi && !found) {
            mid = (lo + hi) / 2;
            Page current = deserialize(Name, mid);
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                lo = mid + 1;
                serialize(current, Name, mid);
                continue;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {
                hi = mid - 1;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) <= 0
                    && Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) >= 0) {
                found = true;
            }
            serialize(current, Name, mid);
        }
        if (!found)
            throw new DBAppException("This record does not exist");
        int i = mid;
        for (; i >= 0; i--) {
            Page current = deserialize(Name, i);
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                serialize(current, Name, i);
                i++;
                break;
            }
            serialize(current, Name, i);
        }
        int size = pages;
        int updated = 1;
        for (int j = Math.max(i, 0); j < size && updated != 0; j++) {
            Page current = null;
            boolean flag = false;
            current = deserialize(Name, j);
            flag = false;
            updated = current.update(HtblColNameValue);
            if (updated != 0) {
                flag = true;
                serialize(current, Name, j);
            }
            if (!flag)
                serialize(current, Name, j);
        }

    }

    public int Compare(Object x, Object y) throws DBAppException {
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
                if (!st[0].equals(Name) || !st[1].equals(Key)) continue;
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
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
        return 0;
    }

    public int CompareInCol(String ColName, Object x, Object y) throws DBAppException {
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        return ((Integer) x).compareTo((Integer) y);
                    case "java.lang.String":
                        return ((String)x).compareTo((String)y);
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
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
        return 0;
    }

    public ArrayList<Record> selectFromTable(String ColName, Object value, String operator) throws DBAppException {
        boolean indexed = ifIndexedBP(ColName);
        boolean key = ifClusteringKey(ColName);
        ArrayList<Record> ans = new ArrayList<>();
        if (operator.equals("!=") || !(indexed || key)) {
            for (int i = 0; i < pages; i++) {
                Page cur = deserialize(Name, i);
                for (int j = 0; j < cur.Rows.size(); j++) {
                    Object originalValue = cur.Rows.get(j).row.get(ColName);
                    int comparison = CompareInCol(ColName, originalValue, value);
                    switch (operator) {
                        case "=":
                            if (comparison == 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case "!=":
                            if (comparison != 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case ">":
                            if (comparison > 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case "<":
                            if (comparison < 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case ">=":
                            if (comparison >= 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case "<=":
                            if (comparison <= 0)
                                ans.add(cur.Rows.get(j));
                            break;


                    }
                }
                serialize(cur, Name, i);
            }
            return ans;
        }
        if (indexed) {
            switch (operator) {
                case "=":
                    ans=EqualToInIndexed(value,ColName);
                    break;
                case ">":
                    ans=BiggerThanInIndexed(value,ColName);
                    break;
                case "<":
                    ans=LessThanInIndexed(value,ColName);
                    break;
                case ">=":
                    ans=CompareOR(BiggerThanInIndexed(value,ColName),EqualToInIndexed(value,ColName));
                    break;
                case "<=":
                    ans=CompareOR(LessThanInIndexed(value,ColName),EqualToInIndexed(value,ColName));
                    break;


            }
            return ans;
        }
        switch (operator) {
            case "=":
                ans=EqualToClustering(value);
                break;
            case ">":
                ans=BiggerThanClustering(value);
                break;
            case "<":
                ans=LessThanClustering(value);
                break;
            case ">=":
                ans=CompareOR(BiggerThanClustering(value),EqualToClustering(value));
                break;
            case "<=":
                ans=CompareOR(LessThanClustering(value),EqualToClustering(value));
                break;


        }


        return ans;
    }

    public ArrayList<Record> CompareAnd(ArrayList<Record> x, ArrayList<Record> y) {
        ArrayList<Record> ans = new ArrayList<>();
        HashSet<Hashtable<String, Object>> test = new HashSet<>();
        for (int i = 0; i < x.size(); i++) {
            test.add(x.get(i).row);
        }
        for (int i = 0; i < y.size(); i++) {
            if (test.contains(y.get(i).row))
                ans.add(y.get(i));
        }
        return ans;
    }

    public ArrayList<Record> CompareOR(ArrayList<Record> x, ArrayList<Record> y) {
        ArrayList<Record> ans = new ArrayList<>();
        HashSet<Hashtable<String, Object>> test = new HashSet<>();
        for (int i = 0; i < x.size(); i++) {
            ans.add(x.get(i));
            test.add(x.get(i).row);
        }
        for (int i = 0; i < y.size(); i++) {
            if (!test.contains(y.get(i).row))
                ans.add(y.get(i));
        }
        return ans;
    }

    public ArrayList<Record> CompareXOR(ArrayList<Record> x, ArrayList<Record> y) {
        ArrayList<Record> ans = new ArrayList<>();
        HashSet<Hashtable<String, Object>> test = new HashSet<>();
        HashSet<Hashtable<String, Object>> test2 = new HashSet<>();
        for (int i = 0; i < x.size(); i++) {
            test.add(x.get(i).row);
        }
        for (int i = 0; i < y.size(); i++) {
            test2.add(y.get(i).row);
        }
        for (int i = 0; i < x.size(); i++) {
            if (!test2.contains(x.get(i).row))
                ans.add(x.get(i));
        }
        for (int i = 0; i < y.size(); i++) {
            if (!test.contains(y.get(i).row))
                ans.add(y.get(i));
        }
        return ans;
    }

    public BPTree createBTreeIndex(String strColName, String type) throws DBAppException {
        BPTree x;
        switch (type) {
            case "java.lang.Integer":
                x = new BPTree<Integer>(Name, strColName);
                break;
            case "java.lang.String":
                x = new BPTree<String>(Name, strColName);
                break;
            case "java.lang.Double":
                x = new BPTree<Double>(Name, strColName);
                break;
            case "java.lang.Boolean":
                x = new BPTree<Boolean>(Name, strColName);
                break;
            case "java.util.Date":
                x = new BPTree<Date>(Name, strColName);
                break;
            default:
                throw new DBAppException("invalid type for B+index");


        }
        for (int i = 0; i < pages; i++) {
            Page curr = deserialize(Name, i);
            for (int j = 0; j < curr.Rows.size(); j++) {
                Ref ref = new Ref(Name + i, j);
                switch (type) {
                    case "java.lang.Integer":
                        x.insert((Integer) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    case "java.lang.String":
                        x.insert((String) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    case "java.lang.Double":
                        x.insert((Double) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    case "java.lang.Boolean":
                        x.insert((Boolean) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    case "java.util.Date":
                        x.insert((Date) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    default:
                        throw new DBAppException("invalid type for B+index");


                }

            }
            serialize(curr, Name, i);
        }

        BPTrees.add(x);
        return x;


    }

    public boolean ifClusteringKey(String colName) {
        return colName.equals(Key);
    }


    public boolean ifIndexedBP(String colName) {
        for (int i = 0; i < BPTrees.size(); i++) {
            if (BPTrees.get(i).ColName.equals(colName))
                return true;
        }
        return false;
    }

    public ArrayList<Record> LessThanClustering(Object value) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        for (int i = 0; i < pages; i++) {
            Page cur = deserialize(Name, i);
            for (int j = 0; j < cur.Rows.size(); j++) {
                Record r = cur.Rows.get(i);
                if (Compare(r.row.get(Key), value) >= 0) {
                    serialize(cur, Name, i);
                    return ans;
                }
                ans.add(r);
            }
            serialize(cur, Name, i);
        }
        return ans;
    }

    public ArrayList<Record> BiggerThanClustering(Object value) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        for (int i = pages - 1; i >= 0; i--) {
            Page cur = deserialize(Name, i);
            for (int j = cur.Rows.size() - 1; j >= 0; j--) {
                Record r = cur.Rows.get(i);
                if (Compare(r.row.get(Key), value) <= 0) {
                    serialize(cur, Name, i);
                    return ans;
                }
                ans.add(r);
            }
            serialize(cur, Name, i);
        }
        return ans;
    }

    public ArrayList<Record> EqualToClustering(Object value) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        int index = FirstEqualInClustering(value);
        if (index == -1)
            return ans;
        for (int i = index; i < pages; i++) {
            Page cur = deserialize(Name, i);
            for (int j = 0; j < cur.Rows.size(); j++) {
                Record r = cur.Rows.get(i);
                if (Compare(r.row.get(Key), value) > 0) {
                    serialize(cur, Name, i);
                    return ans;
                }
                ans.add(r);
            }
            serialize(cur, Name, i);
        }

        return ans;
    }

    public int FirstEqualInClustering(Object value) throws DBAppException {
        int lo = 0;
        int hi = pages - 1;
        int mid = (lo + hi) / 2;
        int ans = -1;
        Page current;
        while (lo <= hi) {
            mid = (lo + hi) / 2;
            current = deserialize(Name, mid);
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), value) < 0) {//kda we will have many pages deserialised rokaya
                lo = mid + 1;
                continue;
            }
            if (Compare(current.Rows.get(0).row.get(Key), value) > 0) {
                hi = mid - 1;
            } else {
                ans = mid;
                hi = mid - 1;
            }
            serialize(current, Name, mid);

        }
        return ans;
    }

    public ArrayList<Record> LessThanInIndexed(Object Value,String ColName) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        BPTree bpTree=null;
        for (int i = 0; i < BPTrees.size(); i++) {
            if(BPTrees.get(i).ColName.equals(ColName)){
                bpTree=BPTrees.get(i);
                break;
            }
        }
        ArrayList<Ref>ansRef=new ArrayList<>();
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        ansRef=bpTree.searchMin((Integer)Value);
                        break;
                    case "java.lang.String":
                        ansRef=bpTree.searchMin((String)Value);
                        break;
                    case "java.lang.Double":
                        ansRef=bpTree.searchMin((Double)Value);
                        break;
                    case "java.lang.Boolean":
                        ansRef=bpTree.searchMin((Boolean)Value);
                        break;
                    case "java.util.Date":
                        ansRef=bpTree.searchMin((Date)Value);
                        break;

                }
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
        for (int i = 0; i <ansRef.size() ; i++) {
           Page now=deserialize(ansRef.get(i).getPage());
           ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));

        }
        return ans;
    }
    public ArrayList<Record> BiggerThanInIndexed(Object Value,String ColName) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        BPTree bpTree=null;
        for (int i = 0; i < BPTrees.size(); i++) {
            if(BPTrees.get(i).ColName.equals(ColName)){
                bpTree=BPTrees.get(i);
                break;
            }
        }
        ArrayList<Ref>ansRef=new ArrayList<>();
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        ansRef=bpTree.searchMax((Integer)Value);
                        break;
                    case "java.lang.String":
                        ansRef=bpTree.searchMax((String)Value);
                        break;
                    case "java.lang.Double":
                        ansRef=bpTree.searchMax((Double)Value);
                        break;
                    case "java.lang.Boolean":
                        ansRef=bpTree.searchMax((Boolean)Value);
                        break;
                    case "java.util.Date":
                        ansRef=bpTree.searchMax((Date)Value);
                        break;

                }
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
        for (int i = 0; i <ansRef.size() ; i++) {
            Page now=deserialize(ansRef.get(i).getPage());
            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));

        }
        return ans;
    }
    public ArrayList<Record> EqualToInIndexed(Object Value,String ColName) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        BPTree bpTree=null;
        for (int i = 0; i < BPTrees.size(); i++) {
            if(BPTrees.get(i).ColName.equals(ColName)){
                bpTree=BPTrees.get(i);
                break;
            }
        }
        ArrayList<Ref>ansRef=new ArrayList<>();
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        ansRef=bpTree.search((Integer)Value);
                        break;
                    case "java.lang.String":
                        ansRef=bpTree.search((String)Value);
                        break;
                    case "java.lang.Double":
                        ansRef=bpTree.search((Double)Value);
                        break;
                    case "java.lang.Boolean":
                        ansRef=bpTree.search((Boolean)Value);
                        break;
                    case "java.util.Date":
                        ansRef=bpTree.search((Date)Value);
                        break;

                }
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
        for (int i = 0; i <ansRef.size() ; i++) {
            Page now=deserialize(ansRef.get(i).getPage());
            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));

        }
        return ans;
    }


}

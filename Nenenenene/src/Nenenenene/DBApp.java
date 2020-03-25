package Nenenenene;

import java.awt.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class DBApp {
    static Vector<Table> tables;
    static Vector<BPTree> bpTrees;
    static PrintWriter out;

    static {

        try {
            out = new PrintWriter(new FileOutputStream("data/metadata.csv", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws DBAppException {
        //out.print("");
        DBApp d = new DBApp();
        d.init();
        Hashtable<String, String> htblcolnametype = new Hashtable<String, String>();
        htblcolnametype.put("ID", "java.lang.Integer");
        htblcolnametype.put("Name", "java.lang.String");
        htblcolnametype.put("Age", "java.lang.Integer");
        d.createTable("Humans", "ID", htblcolnametype);
        Hashtable<String, Object> inputH = new Hashtable<String, Object>();
        inputH.put("ID", 5);
        inputH.put("Name", "Kevin");
        inputH.put("Age", 5);
        d.insertIntoTable("Humans", inputH);
        d.insertIntoTable("Humans", inputH);
        inputH = new Hashtable<String, Object>();
        inputH.put("ID", 5);
        inputH.put("Name", "Kevin");
        inputH.put("Age", 3);
        d.insertIntoTable("Humans", inputH);
        d.insertIntoTable("Humans", inputH);

        char c = 'a';
        for (int i = 0; i < 500; i++) {
            inputH = new Hashtable<String, Object>();
            inputH.put("ID", i);
            c++;
            StringBuilder sb = new StringBuilder();
            sb.append(c);
            sb.append("a");
            sb.append(i);
            inputH.put("Name", sb.toString());
            inputH.put("Age", i % 33);
            d.insertIntoTable("Humans", inputH);
        }
        //d.createBTreeIndex("Humans", "Name");
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0]=new SQLTerm();
        arrSQLTerms[1]=new SQLTerm();
        arrSQLTerms[0].strTableName = "Humans";
        arrSQLTerms[0].strColumnName = "Name";
        arrSQLTerms[0].strOperator = "=";
        arrSQLTerms[0].objValue = "Kevin";
        arrSQLTerms[1].strTableName = "Humans";
        arrSQLTerms[1].strColumnName = "Age";
        arrSQLTerms[1].strOperator = "<";
        arrSQLTerms[1].objValue = new Integer(10);
        String[] strarrOperators = new String[1];
        strarrOperators[0] = "AND";
// select * from Student where name = “John Noor” or gpa = 1.5;
//        ArrayList<Hashtable<String,Object>> resultSet = d.selectFromTable(arrSQLTerms, strarrOperators);
//
//        for (int i = 0; i <resultSet.size() ; i++)
//
//
//            System.out.println(resultSet.get(i));
        // Iterator<Hashtable<String,Object>>res=d.selectFromTable();
//        Hashtable<String, Object> inputH = new Hashtable<String, Object>();
////        inputH.clear();
//        inputH.put("Age", 14);
//        inputH.put("Name", "LINA");
//        d.deleteFromTable("Humans",inputH);
//        inputH.clear(); inputH.put("ID", 2);
//        d.deleteFromTable("Humans", inputH);


//        htblcolnametype = new Hashtable<String, String>();
//        htblcolnametype.put("N", "java.lang.String");
//        htblcolnametype.put("Level", "java.lang.Integer");
//        d.createTable("Sadness", "N", htblcolnametype);
//        inputH = new Hashtable<String, Object>();
//        inputH.put("N", "Hey");
//        inputH.put("Level", 2);
//        d.insertIntoTable("Sadness",inputH);
//        d.createTable("EmptyTable", "N", htblcolnametype);
//        d.printPage("Humans", 0);
//        System.out.println("new page");
//        d.printPage("Human", 0);
//        System.out.println("new page");
//        d.printPage("Humans", 2);
//        System.out.println("new page");
//        d.printPage("Humans", 3);
//        System.out.println("new page");
//        d.printPage("Humans", 4);
//        System.out.println("new page");
//        d.printPage("Humans", 5);
//        System.out.println("new page");
//        d.printPage("Humans", 6);
//        System.out.println("new page");
//        d.printPage("Humans", 7);
//        System.out.println("new page");
//        d.printPage("Humans", 8);
//        System.out.println("New page");
//        d.printPage("Humans", 9);
//        System.out.println("new page");
//        d.printPage("Humans", 10);
//        System.out.println("new page");
//        d.printPage("Humans", 11);
//        System.out.println("new page");
//        d.printPage("Humans", 12);
//        System.out.println("new page");
//        d.printPage("Humans", 13);
//        System.out.println("new page");
//        d.printPage("Humans", 14);
//        System.out.println("new page");
//        d.printPage("Humans", 15);
//        Hashtable<String, Object> inputH = new Hashtable<String, Object>();
//        inputH.put("ID", 205);
//        d.deleteFromTable("Humans", inputH);
//        Page p = d.deserialize("Humans", 0);
//
//        System.err.println(p.Rows.size());
//        d.serialize(p, "Humans", 0);
//        p = d.deserialize("Humans", 1);
//        System.err.println(p.Rows.size());
//        d.serialize(p, "Humans", 1);
//        p = d.deserialize("Humans", 2);
//        System.err.println(p.Rows.size());
//        d.serialize(p, "Humans", 2);
    }

    public void init() throws DBAppException {
        // this does whatever initialization you would like
        // or leave it empty if there is no code you want to
        // execute at application startup
        try {
            FileInputStream fileIn = new FileInputStream("data/Tables.class");
        } catch (FileNotFoundException e) {
            tables = new Vector<>();
            serializeTable();
            out.println("Table Name, Column Name, Column Type, Key, Indexed");
            out.flush();
        }


    }

    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {
        htblColNameType.put("TouchDate", "java.lang.String");
        tables = deserializeTable();
        for (int i = 0; i < tables.size(); i++)
            if (tables.get(i).Name.equals(strTableName)) {
                serializeTable();
                throw new DBAppException("There is a table in the database with that name. PLease choose another name for your table.");
            }
        checkTypesForCreate(htblColNameType);
        Table t = new Table(strClusteringKeyColumn, strTableName);
        tables.add(t);
        //MetaData
        Set<String> keys = htblColNameType.keySet();
        for (String k : keys) {
            out.println(strTableName + ", " + k + ", " + htblColNameType.get(k) + ", " + k.equals(strClusteringKeyColumn) + ", " + false);
        }
        serializeTable();
        out.flush();

    }

    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        boolean tableFound = false;
        tables = deserializeTable();
        for (int i = 0; i < tables.size() && !tableFound; i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                tableFound = true;
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
                Date date = new Date(System.currentTimeMillis());
                htblColNameValue.put("TouchDate", formatter.format(date));
                try {
                    checkForInserts(htblColNameValue, strTableName);
                } catch (IOException e) {
                    serializeTable();
                    throw new DBAppException();
                }
                tables.get(i).insertIntoPage(htblColNameValue);
            }
        }
        serializeTable();
        if (!tableFound) {
            throw new DBAppException("No table with that name in the database");
        }
    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        boolean tableFound = false;
        tables = deserializeTable();
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                tableFound = true;
                try {
                    checkForUpdates(htblColNameValue, strTableName);
                } catch (IOException e) {
                    serializeTable();
                    throw new DBAppException();
                }
                tables.get(i).deleteFromPage(htblColNameValue);
            }
        }
        serializeTable();
        if (!tableFound) {
            serializeTable();
            throw new DBAppException("No table with that name in the database");
        }
    }

    public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        boolean tableFound = false;
        tables = deserializeTable();
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                tableFound = true;
                try {
                    checkForUpdates(htblColNameValue, strTableName);
                } catch (IOException e) {
                    serializeTable();
                    throw new DBAppException();
                }
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
                Date date = new Date(System.currentTimeMillis());
                htblColNameValue.put("TouchDate", formatter.format(date));
                parsingKey(strTableName, strKey, htblColNameValue);
                tables.get(i).updatePage(htblColNameValue);

            }
        }
        serializeTable();
        if (!tableFound) {
            throw new DBAppException("No table with that name in the database");
        }
    }

    public void checkForUpdates(Hashtable<String, Object> htblColNameValue, String name) throws DBAppException, IOException {
        BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
        String s = br.readLine();
        while (br.ready()) {
            s = br.readLine();
            String[] st = s.split(", ");
            if (!st[0].equals(name)) continue;
            String value = st[2];
            String k = st[1];
            //ZEINA CHANGED IT SO THAT THE INPUT HASHTABLE DOESN'T HAVE TO CONTAIN VALUES FOR ALL OF THE COLUMNS
            if (htblColNameValue.containsKey(k)) {
                switch (value) {
                    case "java.lang.Integer":
                        if (!(htblColNameValue.get(k) instanceof Integer)) {
                            throw new DBAppException("This value should be an int");
                        }
                        break;
                    case "java.lang.String":
                        if (!(htblColNameValue.get(k) instanceof String)) {
                            throw new DBAppException("This value should be string");
                        }
                        break;
                    case "java.lang.Double":
                        if (!(htblColNameValue.get(k) instanceof Double)) {
                            throw new DBAppException("This value should be double");
                        }
                        break;
                    case "java.lang.Boolean":
                        if (!(htblColNameValue.get(k) instanceof Boolean)) {
                            throw new DBAppException("This value should be boolean");
                        }
                        break;
                    case "java.awt.Polygon":
                        if (!(htblColNameValue.get(k) instanceof Polygon)) {
                            throw new DBAppException("This value should be polygon");
                        }
                        break;
                    case "java.util.Date":
                        if (!(htblColNameValue.get(k) instanceof Date)) {
                            throw new DBAppException("This value should be date");
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    public void checkForInserts(Hashtable<String, Object> htblColNameValue, String name) throws DBAppException, IOException {
        //The TA SAID THAT FOR INSERTIONS WE NEED TO CHECK THAT ALL COLUMNS ARE INCLUDED
        BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
        String s = br.readLine();
        while (br.ready()) {
            s = br.readLine();
            String[] st = s.split(", ");
            if (!st[0].equals(name)) continue;
            String value = st[2];
            String k = st[1];
            if (!htblColNameValue.containsKey(k)) {
                throw new DBAppException("Hashtable is missing columns");
            } else {
                switch (value) {
                    case "java.lang.Integer":
                        if (!(htblColNameValue.get(k) instanceof Integer)) {
                            throw new DBAppException("This value should be an Integer");
                        }
                        break;
                    case "java.lang.String":
                        if (!(htblColNameValue.get(k) instanceof String)) {
                            throw new DBAppException("This value should be string");
                        }
                        break;
                    case "java.lang.Double":
                        if (!(htblColNameValue.get(k) instanceof Double)) {
                            throw new DBAppException("This value should be Double");
                        }
                        break;
                    case "java.lang.Boolean":
                        if (!(htblColNameValue.get(k) instanceof Boolean)) {
                            throw new DBAppException("This value should be Boolean");
                        }
                        break;
                    case "java.awt.Polygon":
                        if (!(htblColNameValue.get(k) instanceof Polygon)) {
                            throw new DBAppException("This value should be a Polygon");
                        }
                        break;
                    case "java.util.Date":
                        if (!(htblColNameValue.get(k) instanceof Date)) {
                            throw new DBAppException("This value should be Date");
                        }
                        break;

                    default:
                        break;
                }
            }

        }
    }

    public void checkTypesForCreate(Hashtable<String, String> h) throws DBAppException {
        Set<String> keys = h.keySet();
        for (String k : keys) {
            switch (h.get(k)) {
                case "java.lang.Integer":
                    break;
                case "java.lang.String":
                    break;
                case "java.lang.Double":
                    break;
                case "java.lang.Boolean":
                    break;
                case "java.awt.Polygon":
                    break;
                case "java.util.Date":
                    break;
                default:
                    throw new DBAppException("You entered an invalid data type during table creation");
            }
        }
    }

    public void parsingKey(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        BufferedReader br;
        String s = "";
        try {
            BufferedReader bufferedReader = br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("can't find meta data file");
        } catch (IOException IO) {
            throw new DBAppException("can't find metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(strTableName) || !st[3].equals("true")) continue;
                String value = st[2];

                switch (value) {
                    case "java.lang.Integer":
                        Integer x = Integer.parseInt(strKey);
                        htblColNameValue.put(st[1], x);
                        break;
                    case "java.lang.String":
                        htblColNameValue.put(st[1], strKey);
                        break;
                    case "java.lang.Double":
                        htblColNameValue.put(st[1], Double.parseDouble(strKey));
                        break;
                    case "java.lang.Boolean":
                        htblColNameValue.put(st[1], Boolean.parseBoolean(strKey));
                        break;
                    case "java.util.Date":
                        Date date1 = new SimpleDateFormat("YYYY-MM-DD").parse(strKey);
                        htblColNameValue.put(st[1], date1);
                        break;
                    case "java.awt.Polygon":
                        StringBuilder sb = new StringBuilder();
                        int[] x1 = new int[4], y = new int[4];
                        boolean xTurn = true;
                        int counter = 0;
                        for (int i = 0; i < strKey.length(); i++) {
                            if (strKey.charAt(i) >= '0' && strKey.charAt(i) <= '9') {
                                sb.append(strKey.charAt(i));
                            } else {
                                if (sb.length() != 0) {
                                    if (xTurn) {
                                        x1[counter] = Integer.parseInt(sb.toString());
                                        xTurn = false;
                                        sb = new StringBuilder();
                                    } else {
                                        y[counter] = Integer.parseInt(sb.toString());
                                        xTurn = true;
                                        counter++;
                                        sb = new StringBuilder();
                                    }
                                }
                            }
                        }
                        Polygon pol = new Polygon(x1, y, counter);
                        DBPolygon PX = new DBPolygon(pol);
                        htblColNameValue.put(st[1], PX);
                        break;
                    default:
                        throw new DBAppException("Never happens");
                }
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
                Date date = new Date(System.currentTimeMillis());
                htblColNameValue.put("TouchDate", formatter.format(date));
            }
        } catch (IOException e) {
            throw new DBAppException("cant find metadata file");
        } catch (ParseException e) {
            throw new DBAppException("cant parse input");
        }
    }

    //JUST T0 PRINT BEGINS
    public void printPage(String TableName, int pageNumber) throws DBAppException {
        Page p = deserialize(TableName, pageNumber);
        for (int i = 0; i < p.Rows.size(); i++) {
            System.out.println(p.Rows.get(i));
        }
        serialize(p, TableName, pageNumber);
    }

    public Page deserialize(String name, int index) throws DBAppException {
        Page current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/" + name + index + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (Page) in.readObject();
            in.close();
            fileIn.close();
            return current;
        } catch (Exception i) {
            throw new DBAppException("can't find page");
        }
    }

    public void serialize(Page p, String name, int index) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("data/" + name + index + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
            // System.out.printf("Serialized data is saved in /tmp/employee.ser");
        } catch (IOException i) {
            throw new DBAppException("can't serialize page");
        }
    }

    //JUST TO PRINT ENDS
    public Vector<Table> deserializeTable() throws DBAppException {
        Vector<Table> current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/Tables.class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = ((Vector<Table>) in.readObject());
            in.close();
            fileIn.close();
            return current;
        } catch (Exception i) {
            throw new DBAppException("can't find page");
        }
    }

    public void serializeTable() throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/Tables.class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tables);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            throw new DBAppException("Can't serialize object.");
        }
    }

    public Table TableExists(String tableName) throws DBAppException {
        tables = deserializeTable();
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(tableName))
                return tables.get(i);

        }
        return null;
    }

    public  Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        ArrayList<Record> intermediateResult;
        Table queried = TableExists(arrSQLTerms[0].strTableName);
        intermediateResult = queried.selectFromTable(arrSQLTerms[0].strColumnName, arrSQLTerms[0].objValue, arrSQLTerms[0].strOperator);
        for (int i = 1; i < arrSQLTerms.length; i++) {
            queried = TableExists(arrSQLTerms[i].strTableName);
            ArrayList<Record> currentResult;
            currentResult = queried.selectFromTable(arrSQLTerms[i].strColumnName, arrSQLTerms[i].objValue, arrSQLTerms[i].strOperator);
            if (strarrOperators[i - 1].equals("AND")) {
                intermediateResult = queried.CompareAnd(intermediateResult, currentResult);
            } else {
                if (strarrOperators[i - 1].equals("OR"))
                    intermediateResult = queried.CompareOR(intermediateResult, currentResult);
                else intermediateResult = queried.CompareXOR(intermediateResult, currentResult);
            }
        }
        ArrayList<Hashtable<String, Object>> intermediateResult2 = new ArrayList<>();
        for (int i = 0; i < intermediateResult.size(); i++) {
            intermediateResult2.add(intermediateResult.get(i).row);
        }
        return intermediateResult2.iterator();


    }

    public void serializeBPTree() throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/BPTree.class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(bpTrees);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            throw new DBAppException("Can't serialize object.");
        }
    }

    public Vector<BPTree> deserializeBPTree() throws DBAppException {
        Vector<BPTree> current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/BPTree.class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = ((Vector<BPTree>) in.readObject());
            in.close();
            fileIn.close();
            return current;
        } catch (Exception i) {
            throw new DBAppException("can't find page");
        }
    }

    public void createBTreeIndex(String strTableName,
                                 String strColName) throws DBAppException {
        tables = deserializeTable();
        boolean found = false;
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                found = true;
                String type = "";
                try {
                    BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
                    String s = br.readLine();
                    while (br.ready()) {
                        s = br.readLine();
                        String[] st = s.split(", ");
                        if (!st[0].equals(strTableName) || !st[1].equals(strColName)) continue;
                        if (st[4].equals("true"))
                            throw new DBAppException("column already indexed");
                        type = st[2];
                        break;
                    }

                } catch (Exception e) {
                    throw new DBAppException("cannot read from metadata");
                }

                if (type.equals(""))
                    throw new DBAppException("no such column");
                Table t = tables.get(i);
                t.createBTreeIndex(strColName, type);

                serializeTable();
                break;
            }
        }
        if (!found)
            throw new DBAppException("no such table");
    }
}

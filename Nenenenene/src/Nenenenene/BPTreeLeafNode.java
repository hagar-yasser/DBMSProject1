package Nenenenene;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String[] records;
    private BPTreeLeafNode<T> next;
    private BPTreeLeafNode<T> before;

    @SuppressWarnings("unchecked")
    public BPTreeLeafNode(int n, String name) throws DBAppException {
        super(n, name);
        keys = new Comparable[n];
        records = new String[n];
        for (int i = 0; i < n; i++) {
            ArrayList<Ref> r = new ArrayList<Ref>();
            records[i] = serializeRef(r, i);
        }
    }

    /**
     * @return the next leaf node
     */
    public BPTreeLeafNode<T> getNext() {
        return this.next;
    }

    /**
     * @return the before leaf node
     */
    public BPTreeLeafNode<T> getBefore() {
        return this.before;
    }

    /**
     * sets the next leaf node
     *
     * @param node the next leaf node
     */
    public void setNext(BPTreeLeafNode<T> node) {
        this.next = node;
    }

    /**
     * sets the before leaf node
     *
     * @param node the before leaf node
     */
    public void setBefore(BPTreeLeafNode<T> node) {
        this.before = node;
    }

    /**
     * @param index the index to find its record
     * @return the reference of the queried index
     * @throws DBAppException
     */
    public ArrayList<Ref> getRecord(int index) throws DBAppException {
        ArrayList<Ref> r = deserializeRef(records[index]);
        serializeRef(r, index);
        return r;
    }

    /**
     * sets the record at the given index with the passed reference
     *
     * @param index           the index to set the value at
     * @param recordReference the reference to the record
     * @throws DBAppException
     */
    public void setRecord(int index, Ref recordReference) throws DBAppException {
        ArrayList<Ref> r = deserializeRef(records[index]);
        r.add(recordReference);
        serializeRef(r, index);
    }

    /**
     * sets the record at the given index with the passed reference
     *
     * @param index           the index to set the value at
     * @param recordReference the reference to the record
     * @throws DBAppException
     */
    public void setRecord(int index, ArrayList<Ref> recordReference) throws DBAppException {
        ArrayList<Ref> r = deserializeRef(records[index]);
        r = recordReference;
        serializeRef(r, index);
    }

    /**
     * @return the reference of the last record
     * @throws DBAppException
     */
    public ArrayList<Ref> getFirstRecord() throws DBAppException {
        ArrayList<Ref> r = deserializeRef(records[0]);
        serializeRef(r, index);
        return r;
    }

    /**
     * @return the reference of the last record
     * @throws DBAppException
     */
    public ArrayList<Ref> getLastRecord() throws DBAppException {
        ArrayList<Ref> r = deserializeRef(records[numberOfKeys - 1]);
        serializeRef(r, index);
        return r;
    }

    /**
     * finds the minimum number of keys the current node must hold
     */
    public int minKeys() {
        if (this.isRoot())
            return 1;
        return (order + 1) / 2;
    }

    /**
     * insert the specified key associated with a given record refernce in the B+ tree
     *
     * @throws DBAppException
     */
    public PushUp<T> insert(T key, Ref recordReference, BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        if (this.isFull()) {
            BPTreeNode<T> newNode = this.split(key, recordReference);
            Comparable<T> newKey = newNode.getFirstKey();
            return new PushUp<T>(newNode, newKey);
        } else {
            int index = 0;
            while (index < numberOfKeys && getKey(index).compareTo(key) <= 0)
                ++index;
            this.insertAt(index, key, recordReference);
            return null;
        }
    }

    /**
     * inserts the passed key associated with its record reference in the specified index
     *
     * @param index           the index at which the key will be inserted
     * @param key             the key to be inserted
     * @param recordReference the pointer to the record associated with the key
     * @throws DBAppException
     */
    private void insertAt(int index, Comparable<T> key, Ref recordReference) throws DBAppException {
        for (int i = numberOfKeys - 1; i >= index; --i) {
            this.setKey(i + 1, getKey(i));
            this.setRecord(i + 1, getRecord(i));
        }

        this.setKey(index, key);
        this.setRecord(index, recordReference);
        ++numberOfKeys;
    }

    /**
     * inserts the passed key associated with its record references in the specified index
     *
     * @param index           the index at which the key will be inserted
     * @param key             the key to be inserted
     * @param recordReference the pointer to the records associated with the key
     * @throws DBAppException
     */
    private void insertAt(int index, Comparable<T> key, ArrayList<Ref> recordReference) throws DBAppException {
        for (int i = numberOfKeys - 1; i >= index; --i) {
            this.setKey(i + 1, getKey(i));
            this.setRecord(i + 1, getRecord(i));
        }

        this.setKey(index, key);
        this.setRecord(index, recordReference);
        ++numberOfKeys;
    }

    /**
     * splits the current node
     *
     * @param key             the new key that caused the split
     * @param recordReference the reference of the new key
     * @return the new node that results from the split
     * @throws DBAppException
     */
    public BPTreeNode<T> split(T key, Ref recordReference) throws DBAppException {
        int keyIndex = this.findIndex(key);
        int midIndex = numberOfKeys / 2;
        if ((numberOfKeys & 1) == 1 && keyIndex > midIndex)    //split nodes evenly
            ++midIndex;


        int totalKeys = numberOfKeys + 1;
        //move keys to a new node
        BPTreeLeafNode<T> newNode = new BPTreeLeafNode<T>(order, TreeName);
        for (int i = midIndex; i < totalKeys - 1; ++i) {
            newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i));
            numberOfKeys--;
        }

        //insert the new key
        if (keyIndex < totalKeys / 2)
            this.insertAt(keyIndex, key, recordReference);
        else
            newNode.insertAt(keyIndex - midIndex, key, recordReference);

        //set next pointers
        newNode.setNext(this.getNext());
        if(newNode.getNext()!=null)
            newNode.getNext().setBefore(newNode);
        this.setNext(newNode);
        newNode.setBefore(this);


        return newNode;
    }

    /**
     * finds the index at which the passed key must be located
     *
     * @param key the key to be checked for its location
     * @return the expected index of the key
     */
    public int findIndex(T key) {
        for (int i = 0; i < numberOfKeys; ++i) {
            int cmp = getKey(i).compareTo(key);
            if (cmp > 0)
                return i;
        }
        return numberOfKeys;
    }

    /**
     * returns the record reference with the passed key and null if does not exist
     *
     * @throws DBAppException
     */
    @Override
    public ArrayList<Ref> search(T key) throws DBAppException {
        ArrayList<Ref> r = new ArrayList<Ref>();
        for (int i = 0; i < numberOfKeys; ++i)
            if (this.getKey(i).compareTo(key) == 0)
                return this.getRecord(i);
        return r;
    }

    public ArrayList<Ref> searchMin(T key) throws DBAppException {
        ArrayList<Ref> ans = new ArrayList<>();
        BPTreeLeafNode<T> newNode = this;
        while (newNode != null) {
            for (int i = 0; i < newNode.numberOfKeys; ++i) {
                if (newNode.getKey(i).compareTo(key) < 0) {
                    for (int j = 0; j < newNode.getRecord(i).size(); j++) {
                        ans.add(newNode.getRecord(i).get(j));
                    }
                } else return ans;
            }
            newNode = newNode.getNext();
        }
        return ans;
    }

    public ArrayList<Ref> searchMax(T key) throws DBAppException {
        ArrayList<Ref> ans = new ArrayList<>();
        BPTreeLeafNode<T> newNode = this;
        while (newNode != null) {
            for (int i = 0; i < newNode.numberOfKeys; ++i) {
                if (newNode.getKey(i).compareTo(key) > 0) {
                    for (int j = 0; j < newNode.getRecord(i).size(); j++) {
                        ans.add(newNode.getRecord(i).get(j));
                    }
                } else return ans;
            }
            newNode = newNode.getBefore();
        }
        return ans;
    }


    /**
     * delete the passed key from the B+ tree
     *
     * @throws DBAppException
     */
    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        for (int i = 0; i < numberOfKeys; ++i)
            if (keys[i].compareTo(key) == 0) {
                this.deleteAt(i);
                if (i == 0 && ptr > 0) {
                    //update key at parent
                    parent.setKey(ptr - 1, this.getFirstKey());
                }
                //check that node has enough keys
                if (!this.isRoot() && numberOfKeys < this.minKeys()) {
                    //1.try to borrow
                    if (borrow(parent, ptr))
                        return true;
                    //2.merge
                    merge(parent, ptr);
                }
                return true;
            }
        return false;
    }

    /**
     * delete a key at the specified index of the node
     *
     * @param index the index of the key to be deleted
     */
    public void deleteAt(int index) {
        for (int i = index; i < numberOfKeys - 1; ++i) {
            keys[i] = keys[i + 1];
            records[i] = records[i + 1];
        }
        numberOfKeys--;
    }

    /**
     * tries to borrow a key from the left or right sibling
     *
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     * @return true if borrow is done successfully and false otherwise
     * @throws DBAppException
     */
    public boolean borrow(BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        //check left sibling
        if (ptr > 0) {
            BPTreeNode<T> child = deserializeNode(parent.getChild(ptr - 1));
            BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) child;
            if (leftSibling.numberOfKeys > leftSibling.minKeys()) {
                this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord());
                leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
                parent.setKey(ptr - 1, keys[0]);
                serializeNode(leftSibling);
                return true;
            }
            serializeNode(leftSibling);
        }

        //check right sibling
        if (ptr < parent.numberOfKeys) {
            BPTreeNode<T> child = deserializeNode(parent.getChild(ptr + 1));
            BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) child;
            if (rightSibling.numberOfKeys > rightSibling.minKeys()) {
                this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord());
                rightSibling.deleteAt(0);
                parent.setKey(ptr, rightSibling.getFirstKey());
                serializeNode(rightSibling);
                return true;
            }
            serializeNode(rightSibling);
        }
        return false;
    }

    /**
     * merges the current node with its left or right sibling
     *
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     * @throws DBAppException
     */
    public void merge(BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        if (ptr > 0) {
            //merge with left
            BPTreeNode<T> child = deserializeNode(parent.getChild(ptr - 1));
            BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) child;
            leftSibling.merge(this);
            parent.deleteAt(ptr - 1);
            serializeNode(leftSibling);
        } else {
            //merge with right
            BPTreeNode<T> child = deserializeNode(parent.getChild(ptr + 1));
            BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) child;
            this.merge(rightSibling);
            parent.deleteAt(ptr);
            serializeNode(rightSibling);
        }
    }

    /**
     * merge the current node with the specified node. The foreign node will be deleted
     *
     * @param foreignNode the node to be merged with the current node
     */
    public void merge(BPTreeLeafNode<T> foreignNode) throws DBAppException {
        for (int i = 0; i < foreignNode.numberOfKeys; ++i)
            this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i));

        this.setNext(foreignNode.getNext());
        if(this.getNext()!=null)
        this.getNext().setBefore(this);
    }


    public ArrayList<Ref> deserializeRef(String s) throws DBAppException {
        ArrayList<Ref> current = null;
        try {
            FileInputStream fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (ArrayList<Ref>) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No Ref file with this name");
        }
        return current;
    }

    public String serializeRef(ArrayList<Ref> n, int ind) throws DBAppException {
        try {
            String s = getFilePath(ind);
            FileOutputStream fileOut = new FileOutputStream(s);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(n);
            out.close();
            fileOut.close();
            return s;
        } catch (IOException i) {
            throw new DBAppException("Can not serialize Ref");
        }
    }

    public String getFilePath(int i) {
        String s = "data/" + TreeName + this.index + "Ref" + i + ".class";
        return s;
    }
}
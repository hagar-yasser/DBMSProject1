package Nenenenene;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

public class BPTree<T extends Comparable<T>> implements Serializable{
	Properties config;
	private static final long serialVersionUID = 1L;
	private int order;
	private BPTreeNode<T> root;
	private String TableName;
	public String ColName;
	/**
     * The branching factor or order for the B+ tree, that measures the capacity of nodes
     * (i.e., the number of children nodes) for internal nodes in the tree.
     */


    public void readProperty() throws DBAppException {
        config = new Properties();
        try {
            FileInputStream f = new FileInputStream("config/DBApp.properties");
            config.load(f);
        } catch (IOException e) {
            throw new DBAppException("Problem with reading the config file.");
        }

    }
    /* config file has the branching factor*/
    
    /**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
     * @throws DBAppException 
	 */
	public BPTree(String table, String column) throws DBAppException 
	{
        readProperty();
        String s = config.getProperty("NodeSize");
        order = Integer.parseInt(s);
        ColName = column;
		TableName = table + "_" + column;
		root = new BPTreeLeafNode<T>(order, TableName);
		root.setRoot(true);
	}
	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 * @throws DBAppException 
	 */
	public void insert(T key, Ref recordReference) throws DBAppException
	
	{
		//System.out.println(key);
		PushUp<T> pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			//System.out.println("lala");
			BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order, TableName);
			newRoot.insertLeftAt(0, pushUp.key, root.getFilePath());
			newRoot.setChild(1, pushUp.newNode.getFilePath());
			root.setRoot(false);
			root.serializeNode(root);
			root = newRoot;
			root.setRoot(true);
		}
	}
	
	
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 * @throws DBAppException 
	 */
	public ArrayList<Ref> search(T key) throws DBAppException
	{
		return root.search(key);
	}
	
	public boolean updateRef(T key, Ref oldRef, Ref newRef) throws DBAppException{
		return root.updateRef(key, oldRef, newRef);
	}
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 * @throws DBAppException 
	 */
	public boolean deleteEntireKey(T key) throws DBAppException
	{
		boolean done = root.deleteEntireKey(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot()){
			String st = ((BPTreeInnerNode<T>) root).getFirstChild();
			root = deserializeNode(st);
		}
		return done;
	}
	
	public boolean deleteSingleRef(T key, Ref r) throws DBAppException
	{
		boolean done = root.deleteSingleRef(key, null, -1, r);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot()){
			String st = ((BPTreeInnerNode<T>) root).getFirstChild();
			root = deserializeNode(st);
		}
		return done;
	}
	
	public BPTreeNode<T> deserializeNode(String s) throws DBAppException {
        BPTreeNode<T> current = null;
        try {
            FileInputStream fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (BPTreeNode<T>) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No Node file with this name  "+s);
        }
        return current;
    }
	
	public ArrayList<Ref> searchMin(T key) throws DBAppException {
        return root.searchMin(key);
    }

    public ArrayList<Ref> searchMax(T key) throws DBAppException {
        return root.searchMax(key);
    }
    
    public static void main(String[] args) 
	{
//		BPTree<Integer> tree = new BPTree<Integer>("hell", "o")
//		Scanner sc = new Scanner(System.in);
//		while(true) 
//		{
//			int x = sc.nextInt();
//			if(x == -1)
//				break;
//			tree.insert(x, null);
//			System.out.println(tree.toString());
//		}
		
//		sc.close();
	}	
	
	/**
	 * Returns a string representation of the B+ tree.
	 */
    public String toString1() throws DBAppException
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
		cur.add(root);
		boolean flag=false;
		while(!cur.isEmpty())
		{
			next = new LinkedList<BPTreeNode<T>>();
			while(!cur.isEmpty())
			{
				if(!flag) {
					BPTreeNode<T> curNode = cur.remove();
					System.out.print(curNode.toString());
					
					if(curNode instanceof BPTreeLeafNode) {
						System.out.print("->");
					}
					else
					{
						System.out.print("{");
						BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
						for(int i = 0; i <= parent.numberOfKeys; ++i)
						{
						  //System.out.println(i);
							BPTreeNode<T> x = (BPTreeNode<T>) deserializeNode(parent.getChild(i));
							
							System.out.print(x.index+",");
							x.serializeNode(x);
							next.add(x);
						}
						System.out.print("} ");
						System.out.println();
					}
					
				
				
				cur = next;
				
			//	</For Testing>
				flag=true;
						
				}
				else {
				BPTreeNode<T> curNode = deserializeNode(cur.remove().getFilePath());
				System.out.print(curNode.toString());
			       
				if(curNode instanceof BPTreeLeafNode) {
					System.out.print("->");
				    curNode.serializeNode(curNode);
				}
			
				else
				{
					curNode.serializeNode(curNode);
					System.out.print("{");
					BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{
						BPTreeNode<T> x = (BPTreeNode<T>) deserializeNode(parent.getChild(i));
						
						System.out.print(x.index+",");
						x.serializeNode(x);
						next.add(x);
					}
					System.out.print("} ");
					System.out.println();
				
				
			}
			
			cur = next;
		}
		}
		}
		//	</For Testing>
		return s;
	}
}
package ca.uvic.csc.research;

import java.lang.String;
import java.lang.Integer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.ArrayDeque;

import org.apache.commons.lang.ArrayUtils; //packed with Hadoop

/* This is a class implementing an Aho-Corasick keyword tree for pattern matching. */

class ACKeywordTree<T> { 
	private ACKeywordTreeNode<T> root;
	private boolean tree_finalized;
	
	public ACKeywordTree(){
		root = new ACKeywordTreeNode<T>(null);
		tree_finalized = false;
	}
	
	public void addKeyword(T[] keyword){
		/*return*/ addKeyword(keyword, 0, keyword.length);
	}
		
	private void addKeyword(T[] keyword, int startIndex, int endIndexExclusive){
		ACKeywordTreeNode<T> pointer = this.root;
		ACKeywordTreeNode<T> nextPointer = null;
		boolean branch_mode = false;
		for(int i = startIndex; i<endIndexExclusive; i++){
			if(!branch_mode){
				nextPointer = pointer.moveToChild(keyword[i]);
			}
			if(branch_mode || nextPointer == null){
				branch_mode = true; //we're splitting off a new branch, so we'll just go straight here from hereon
				pointer = pointer.addChild(new ACKeywordTreeNode<T>(keyword[i]));
			} else {
				pointer = nextPointer;
			}
		}
		pointer.inDict = true;
	}
	
	public void addChunkedKeyword(T[] keyword, int chunk_size){
		int l = keyword.length;
		for(int i = 0; i <= l-chunk_size; i++){
			addKeyword(keyword, i, i+chunk_size);
		}
	}
	
	public boolean testKeyword(T[] keyword){
		ACKeywordTreeNode<T> pointer = this.root;
		int i;
		for(i = 0; i<keyword.length && pointer != null; i++){
			pointer = pointer.moveToChild(keyword[i]);	
		}
		if(i<keyword.length){
			return false; //ended early
		}
		return true; //we're done here
	}
	
	public ArrayList<ACKeywordTreeNode<T>> testAllKeywords(T[] keywords){
		if(!tree_finalized){
			addSuffixLinks();
		}
		ACKeywordTreeNode<T> pointer = this.root;
		ACKeywordTreeNode<T> nextPointer;
		ArrayList<ACKeywordTreeNode<T>> foundList = new ArrayList<ACKeywordTreeNode<T>>();
		for(int i = 0; i<keywords.length && pointer != null; i++){
			//System.out.println("Next symbol: "+keywords[i]);
			//System.out.println("Current node: "+pointer.path());
			nextPointer = pointer.moveToChild(keywords[i]);	
			/*if(pointer.parent == null && nextPointer = null){
				continue; //we're at the root, and there is no match. Keep moving on.
			}*/
			while(nextPointer == null){
				if(pointer.parent == null){ 
					break; //we're at the root, and there is no match. Keep moving on.
				} else if(pointer.fallthrough == null){
					System.err.println("Error: An unlinked node: "+pointer.path());
					System.exit(2);
				}
			
				pointer = pointer.fallthrough;
				nextPointer = pointer.moveToChild(keywords[i]);	
				/*if(pointer == null && nextPointer == null ){
					//give up
					return foundList;
				} else {*/
				//System.out.println("Moving sideways to "+pointer.path()+".");
				// }*/
			}
			if(nextPointer == null){ //we broke out b/c we're at the root
				continue;
			}
			//System.out.println("Moving down to "+nextPointer.path()+".");
			pointer = nextPointer;
			if(pointer.inDict){
				foundList.add(pointer);
			}
			//now check the output link
			if(pointer.falldict != null){
				foundList.add(pointer.falldict);
			}
		}
		return foundList;
	}
	
	public boolean testAllKeywordsShortCircuit(T[] keywords){
		if(!tree_finalized){
			addSuffixLinks();
		}
		//short-circuit version - returns true if any keyword is found in the tree.
		ACKeywordTreeNode<T> pointer = this.root;
		ACKeywordTreeNode<T> nextPointer;
		ArrayList<ACKeywordTreeNode<T>> foundList = new ArrayList<ACKeywordTreeNode<T>>();
		for(int i = 0; i<keywords.length && pointer != null; i++){
			nextPointer = pointer.moveToChild(keywords[i]);	
			while(nextPointer == null){
				if(pointer.parent == null){ 
					break; //we're at the root, and there is no match. Keep moving on.
				} else if(pointer.fallthrough == null){
					System.err.println("Error: An unlinked node: "+pointer.path());
					System.exit(2);
				}
			
				pointer = pointer.fallthrough;
				nextPointer = pointer.moveToChild(keywords[i]);	
				//System.out.println("Moving sideways to "+pointer.path()+".");
			}
			if(nextPointer == null){ //we broke out b/c we're at the root
				continue;
			}
			//System.out.println("Moving down to "+nextPointer.path()+".");
			pointer = nextPointer;
			if(pointer.inDict){
				return true;
				//foundList.add(pointer);
			}
			//now check the output link
			if(pointer.falldict != null){
				return true;
				//foundList.add(pointer.falldict);
			}
		}
		return false;
	}
	
	public String toString(){
		ArrayDeque<ACKeywordTreeNode<T>> bfs_queue = new ArrayDeque<ACKeywordTreeNode<T>>();
		ACKeywordTreeNode<T> nextNode = null;
		bfs_queue.add(this.root);
		int this_level = 1;
		int next_level = 0;
		int level = 0;
		String str = "";
		
		while(!bfs_queue.isEmpty()){
			nextNode = bfs_queue.remove();
			str += "Level "+level+": ";
			if(level == 0){
				str += "Root node.\n";
			} else {
				str += "Node '"+nextNode.token+"'\n";
			}
			this_level--;
			str+="Children:";
			
			if(nextNode.children.size() == 0){
				str += " None (leaf node)\n";
			} else {
				str += "\n";
				for(ACKeywordTreeNode<T> node : nextNode.children.values()){
					bfs_queue.add(node);
					str+="Node - token "+node.token+", "+node.children.size()+" children\n";
					next_level++;
				}
			}
			if(this_level == 0){
				this_level = next_level;
				level++;
				next_level = 0;
			}
		}
		return str;
	}
	
	public void addSuffixLinks(){
		ArrayDeque<ACKeywordTreeNode<T>> bfs_queue = new ArrayDeque<ACKeywordTreeNode<T>>();
		bfs_queue.add(this.root);
		tree_finalized = true;
		
		ACKeywordTreeNode<T> nextNode, pointer, childPointer;
		while(!bfs_queue.isEmpty()){
			nextNode = bfs_queue.remove();
			//generate suffix link
			
			if(nextNode.parent == this.root){ //if it's a child of the root, the suffix link ALWAYS goes to the root. Problem solved.
				nextNode.fallthrough = this.root;
				//System.out.println("Linking (blue) from '"+nextNode.path()+"' to root.");
				//there's no output link - the root is never in the dictionary.
			} else if(nextNode.parent != null){ //if it's null, it'd BE the root, and we'd give up.
				T current_token = nextNode.token;
				pointer = nextNode.parent; //move to parent of current node as starting position in search
				while(pointer != this.root){
					pointer = pointer.fallthrough; //fall through
					//now check children
					childPointer = pointer.moveToChild(current_token);
					if(childPointer != null){
						pointer = childPointer; 
						break; //move to set stage
					}
				}
				nextNode.fallthrough = pointer; //done searching, set
				
				/*if(nextNode.fallthrough != null){
					System.out.println("Linking (blue) from '"+nextNode.path()+"' to '"+nextNode.fallthrough.path()+"'");
				} else {
					System.out.println("No blue link available.");
				}*/
				//now: find the output link.
				if(pointer.inDict){
					nextNode.falldict = pointer; //that was easy
				} else {
					nextNode.falldict = pointer.falldict; //if 'pointer' has an output link, this copies it. If it's null, then it copies the absence of an output link! Win-win.
				}
				/*if(nextNode.falldict != null){
					System.out.println("Linking (dict) from '"+nextNode.path()+"' to '"+nextNode.falldict.path()+"'");
				} else {
					System.out.println("No dict link available.");
				}*/
			}
			//expand successors
			for(ACKeywordTreeNode<T> node : nextNode.children.values()){
				bfs_queue.add(node);
			}
		}
		
	}
	
	public static void main(String[] args){ //test suite
		ACKeywordTree<Character> tree = new ACKeywordTree<Character>();
		ACKeywordTree<Character> tree2 = new ACKeywordTree<Character>();
		ACKeywordTree<Character> tree3 = new ACKeywordTree<Character>();
		System.out.println("Tree 1:");
		tree.addKeyword(ArrayUtils.toObject("a".toCharArray()));
		tree.addKeyword(ArrayUtils.toObject("ab".toCharArray()));
		//System.out.println(tree);
		tree.addKeyword(ArrayUtils.toObject("bab".toCharArray()));
		tree.addKeyword(ArrayUtils.toObject("bc".toCharArray()));
		//System.out.println(tree);
		tree.addKeyword(ArrayUtils.toObject("bca".toCharArray()));
		tree.addKeyword(ArrayUtils.toObject("c".toCharArray()));
		//System.out.println(tree);
		tree.addKeyword(ArrayUtils.toObject("caa".toCharArray()));
		System.out.println(tree);
		System.out.println();
		System.out.println("Tree 2:");
		tree2.addChunkedKeyword(ArrayUtils.toObject("bcaa".toCharArray()),3); //tags bca, caa
		tree2.addKeyword(ArrayUtils.toObject("ab".toCharArray())); //tags ab
		tree2.addKeyword(ArrayUtils.toObject("bab".toCharArray())); //tags bab
		tree2.addKeyword(ArrayUtils.toObject("bc".toCharArray())); //tags bc
		tree2.addChunkedKeyword(ArrayUtils.toObject("ac".toCharArray()),1); //tags a, c
		System.out.println(tree2);
		System.out.println();
		System.out.println("Suffixes 1:");
		tree.addSuffixLinks();
		System.out.println();
		System.out.println("Suffixes 2:");
		tree2.addSuffixLinks();
		System.out.println();
		System.out.println("Test 1:");
		ArrayList<ACKeywordTreeNode<Character>> s = tree.testAllKeywords(ArrayUtils.toObject("abccab".toCharArray()));
		for(ACKeywordTreeNode<Character> t : s){
			System.out.print(t.path()+" ");
		}
		System.out.println();
		System.out.println("Test 2:");
		s = tree2.testAllKeywords(ArrayUtils.toObject("abccab".toCharArray()));
		for(ACKeywordTreeNode<Character> t : s){
			System.out.print(t.path()+" ");
		}
		System.out.println();
		
		System.out.println("Test 3:");
		tree3.addKeyword(ArrayUtils.toObject("ism".toCharArray()));
		
		s = tree3.testAllKeywords(ArrayUtils.toObject("antidisestablishmentarianism".toCharArray()));
		for(ACKeywordTreeNode<Character> t : s){
			System.out.print(t.path()+" ");
		}
		System.out.println();
	}
	
}


class ACKeywordTreeNode<T> {
	public T token;
	public ACKeywordTreeNode<T> parent;	
	public ACKeywordTreeNode<T> fallthrough; //failure link
	public ACKeywordTreeNode<T> falldict; //output link
	public HashMap<T,ACKeywordTreeNode<T>> children;
	public boolean inDict;
	
	public ACKeywordTreeNode(){
		this.token = null;
		this.parent = null;
		this.children = new HashMap<T,ACKeywordTreeNode<T>>();
		this.fallthrough = null;
		this.falldict = null;
		this.inDict = false;
	}
	
	public ACKeywordTreeNode(T token){
		this();
		this.token = token;
	}
	
	public ACKeywordTreeNode(T token, ACKeywordTreeNode<T> parent){
		this();
		this.token = token;
		this.parent = parent;
	}
	
	public ACKeywordTreeNode<T> getParent(){
		return parent;
	}
	public void setParent(ACKeywordTreeNode<T> parent){
		this.parent = parent;
	}
	public ACKeywordTreeNode<T> addChild(ACKeywordTreeNode<T> newChild){ //returns the child node
		//you can overload on return types, right?
		children.put(newChild.token,newChild);
		newChild.setParent(this);
		return newChild;
	}
	
	public ACKeywordTreeNode<T> moveToChild(T token){
		return children.get(token); //returns null if not found
	}
	
	public String toString(){
		String str = "Node with token "+token+", "+children.size()+" children. Node is ";
		if(inDict){ str += "NOT "; }
		str += "in dictionary.\n";
		return str;
	}
	
	public String path(){
		String str = "";
		ACKeywordTreeNode<T> pointer = this;
		while(pointer.parent != null){
			str = pointer.token + str;
			pointer = pointer.parent;
		}
		return str;
	}
	
	/*public int delChildAt(int index){
		children.
	}*/
	
}
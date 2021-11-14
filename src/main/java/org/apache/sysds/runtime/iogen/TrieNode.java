package org.apache.sysds.runtime.iogen;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

class TrieNode {
	private final Map<Character, TrieNode> children = new HashMap<>();
	private boolean endOfWord;
	private ArrayList<Integer> rowIndexes;
	private BitSet rowIndexesBitSet;

	public TrieNode() {
		rowIndexes = new ArrayList<>();
		rowIndexesBitSet = new BitSet();
	}

	public void addRowIndex(int rowIndex){
		rowIndexes.add(rowIndex);
	}

	Map<Character, TrieNode> getChildren() {
		return children;
	}

	boolean isEndOfWord() {
		return endOfWord;
	}

	void setEndOfWord(boolean endOfWord) {
		this.endOfWord = endOfWord;
	}

	public int getRowIndex() {
		for(int i=0; i<rowIndexes.size(); i++){
			int index = rowIndexes.get(i);
			if(!rowIndexesBitSet.get(index))
				return index;
		}
		return -1;
	}

	public void setRowIndexUsed(int rowIndex){
		this.rowIndexesBitSet.set(rowIndex);
	}
}

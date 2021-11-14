package org.apache.sysds.runtime.iogen;


class Trie {
	private TrieNode root;

	Trie() {
		root = new TrieNode();
	}

	void insert(String word, int rowIndex) {
		TrieNode current = root;

		for(char l : word.toCharArray()) {
			current = current.getChildren().computeIfAbsent(l, c -> new TrieNode());
			current.addRowIndex(rowIndex);
		}
		current.setEndOfWord(true);
	}

	TrieNode containsString(String word) {
		TrieNode current = root;
		for(int i = 0; i < word.length(); i++) {
			char ch = word.charAt(i);
			TrieNode node = current.getChildren().get(ch);
			if(node == null) {
				return null;
			}
			current = node;
		}
		return current;
	}

	int containsStringAndSet(String word) {
		TrieNode result = containsString(word);
		int rowIndex =  -1;
		if(result!=null) {
			rowIndex = result.getRowIndex();
			if(rowIndex != -1)
				result.setRowIndexUsed(rowIndex);
		}
		return rowIndex;
	}

}

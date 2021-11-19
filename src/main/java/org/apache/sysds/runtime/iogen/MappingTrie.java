/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.iogen;

public class MappingTrie {
	private MappingTrieNode root;

	public MappingTrie() {
		root = new MappingTrieNode();
	}

	public void insert(String word, int rowIndex) {
		MappingTrieNode current = root;

		for(char l : word.toCharArray()) {
			current = current.getChildren().computeIfAbsent(l, c -> new MappingTrieNode());
			current.addRowIndex(rowIndex);
		}
		current.setEndOfWord(true);
	}

	public MappingTrieNode containsString(String word) {
		MappingTrieNode current = root;
		for(int i = 0; i < word.length(); i++) {
			char ch = word.charAt(i);
			MappingTrieNode node = current.getChildren().get(ch);
			if(node == null) {
				return null;
			}
			current = node;
		}
		return current;
	}

	public int containsStringAndSet(String word) {
		MappingTrieNode result = containsString(word);
		int rowIndex =  -1;
		if(result!=null) {
			rowIndex = result.getRowIndex();
			if(rowIndex != -1)
				result.setRowIndexUsed(rowIndex);
		}
		return rowIndex;
	}

}

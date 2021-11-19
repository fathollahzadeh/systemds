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

import java.util.ArrayList;

public class MappingTrieString {
	private MappingTrieNodeString root;

	public MappingTrieString() {
		root = new MappingTrieNodeString();
	}

	private String intersect(String str1, String str2) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < Math.min(str1.length(), str2.length()); i++) {
			if(str1.charAt(i) == str2.charAt(i))
				sb.append(str1.charAt(i));
			else
				break;
		}
		if(sb.length() == 0)
			return null;
		else
			return sb.toString();
	}

	private TrieNodeResult getSubNode(MappingTrieNodeString current, String word) {
		for(String key : current.getChildren().keySet()) {
			String insec = intersect(key, word);
			if(insec != null)
				return new TrieNodeResult(current.getChildren().get(key), insec, key);
		}
		return null;
	}

	public void insert(String word, int rowIndex) {
		MappingTrieNodeString current = root;
		String remindKeyWord;
		String currentWord = word;
		TrieNodeResult trieNodeResult;
		do {
			trieNodeResult = getSubNode(current, currentWord);
			if(trieNodeResult == null) {
				MappingTrieNodeString newNode = new MappingTrieNodeString();
				newNode.addRowIndex(rowIndex);
				current.getChildren().put(currentWord, newNode);
			}
			else {
				currentWord = currentWord.substring(trieNodeResult.intersect.length());
				remindKeyWord = trieNodeResult.nodeKey.substring(trieNodeResult.intersect.length());
				int cwl = currentWord.length();
				int rkwl = remindKeyWord.length();

				if(cwl == 0) {
					if(rkwl == 0) {
						trieNodeResult.trieNode.addRowIndex(rowIndex);
					}
					else {
						MappingTrieNodeString newNode = new MappingTrieNodeString();

						MappingTrieNodeString updateNode = new MappingTrieNodeString();
						updateNode.setChildren(trieNodeResult.trieNode.getChildren());
						updateNode.setRowIndexes(trieNodeResult.trieNode.getRowIndexes());

						// Add Update Node
						newNode.getChildren().put(remindKeyWord, updateNode);

						// Add New Node
						newNode.setRowIndexes(new ArrayList<>(trieNodeResult.trieNode.getRowIndexes()));
						newNode.addRowIndex(rowIndex);
						current.getChildren().put(trieNodeResult.intersect, newNode);

						// Remove old node
						current.getChildren().remove(trieNodeResult.nodeKey);
					}

				}
				else if(rkwl == 0) {
					current = trieNodeResult.trieNode;
					current.addRowIndex(rowIndex);
				}
				else {

					MappingTrieNodeString newNode = new MappingTrieNodeString();

					MappingTrieNodeString updateNode = new MappingTrieNodeString();
					updateNode.setChildren(trieNodeResult.trieNode.getChildren());
					updateNode.setRowIndexes(trieNodeResult.trieNode.getRowIndexes());

					// Add Update Node
					newNode.getChildren().put(remindKeyWord, updateNode);

					// Add New Node
					newNode.setRowIndexes(new ArrayList<>(trieNodeResult.trieNode.getRowIndexes()));
					newNode.addRowIndex(rowIndex);
					current.getChildren().put(trieNodeResult.intersect, newNode);

					// Remove old node
					current.getChildren().remove(trieNodeResult.nodeKey);

					// Add New Word Remind
					MappingTrieNodeString newWordNode = new MappingTrieNodeString();
					newWordNode.addRowIndex(rowIndex);
					newNode.getChildren().put(currentWord, newWordNode);

					break;
				}
			}

		}
		while(trieNodeResult != null && currentWord.length() > 0);

	}

	public MappingTrieNodeString containsString(String word) {
		MappingTrieNodeString current = root;
		String currentWord = word;
		do {
			TrieNodeResult trieNodeResult = getSubNode(current, currentWord);
			if(trieNodeResult == null)
				return null;
			else {
				currentWord = currentWord.substring(trieNodeResult.intersect.length());
				current = trieNodeResult.trieNode;
				if(currentWord.length() == 0)
					break;
			}
		}
		while(currentWord.length() > 0);

		return current;
	}

	public int containsStringAndSet(String word) {
		MappingTrieNodeString result = containsString(word);
		int rowIndex = -1;
		if(result != null) {
			rowIndex = result.getRowIndex();
			if(rowIndex != -1)
				result.setRowIndexUsed(rowIndex);
		}
		return rowIndex;
	}

	private class TrieNodeResult {
		MappingTrieNodeString trieNode;
		String intersect;
		String nodeKey;

		public TrieNodeResult(MappingTrieNodeString trieNode, String intersect, String nodeKey) {
			this.trieNode = trieNode;
			this.intersect = intersect;
			this.nodeKey = nodeKey;
		}

		public MappingTrieNodeString getTrieNode() {
			return trieNode;
		}

		public void setTrieNode(MappingTrieNodeString trieNode) {
			this.trieNode = trieNode;
		}

		public String getIntersect() {
			return intersect;
		}

		public void setIntersect(String intersect) {
			this.intersect = intersect;
		}
	}

}

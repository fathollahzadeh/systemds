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
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class MappingTrieNodeString {
	private Map<String, MappingTrieNodeString> children = new HashMap<>();
	private ArrayList<Integer> rowIndexes;
	private BitSet rowIndexesBitSet;

	public MappingTrieNodeString() {
		rowIndexes = new ArrayList<>();
		rowIndexesBitSet = new BitSet();
	}

	public void addRowIndex(int rowIndex) {
		rowIndexes.add(rowIndex);
	}

	public Map<String, MappingTrieNodeString> getChildren() {
		return children;
	}

	public int getRowIndex() {
		for(int i = 0; i < rowIndexes.size(); i++) {
			int index = rowIndexes.get(i);
			if(!rowIndexesBitSet.get(index))
				return index;
		}
		return -1;
	}

	public void setRowIndexUsed(int rowIndex) {
		this.rowIndexesBitSet.set(rowIndex);
	}

	public void setChildren(Map<String, MappingTrieNodeString> children) {
		this.children = children;
	}

	public void setRowIndexes(ArrayList<Integer> rowIndexes) {
		this.rowIndexes = rowIndexes;
	}

	public ArrayList<Integer> getRowIndexes() {
		return rowIndexes;
	}
}

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

package org.apache.sysds.runtime.iogen.template;

import org.apache.sysds.common.Types;

public class CodeGenTrie {

	public CodeGenTrieNode root;

	public CodeGenTrie() {
		root = new CodeGenTrieNode(-1, "ROOT");
	}

	public void insert(String condition, int colIndex, Types.ValueType valueType) {
		CodeGenTrieNode current = root;
		String[] conditionLevels = condition.split("\\.");
		for(String cl : conditionLevels) {
			current = current.getChildren().computeIfAbsent(cl, c -> new CodeGenTrieNode(colIndex, cl));
		}
		current.setEndOfCondition(true);
		current.setValueType(valueType);
	}
}

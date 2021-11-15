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

package org.apache.sysds.runtime.iogen.template.javajson;

import com.google.gson.Gson;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.iogen.CustomProperties;
import org.apache.sysds.runtime.iogen.template.TemplateBase;
import org.apache.sysds.runtime.matrix.data.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TemplateJavaJSON extends TemplateBase {

	private String code = "%code%";
	private String javaJSONTemplate;

	public TemplateJavaJSON(CustomProperties _props) {
		super(_props);
		javaJSONTemplate = code;

	}

	public String getFrameReaderCode() {

		String[] colKeys = _props.getColKeys();
		Types.ValueType[] schema = _props.getSchema();

		Trie trie = new Trie();

		int colIndex = 0;
		for(String ck : colKeys) {
			trie.insert(ck, colIndex++);
		}

		Gson gson = new Gson();
		System.out.println(gson.toJson(trie));

		//		HashMap<String, TreeNode> roots = new HashMap<>();
		//		int colIndex = 0;
		//		for(String ck : colKeys) {
		//
		//			Types.ValueType vt = schema[colIndex];
		//			String[] keys = ck.split("\\.");
		//
		//			TreeNode root;
		//			if(!roots.containsKey(keys[0])) {
		//				root = new TreeNode(keys[0], vt, colIndex);
		//				root.jsonObject = "jsonObject0";
		//				roots.put(keys[0], root);
		//			}
		//			else
		//				root = roots.get(keys[0]);
		//
		//			int l = 1;
		//			for(int i = 1; i < keys.length; i++) {
		//				String jsonObject;
		//				String key = keys[i];
		//				if(isNumeric(key))
		//					jsonObject = "JSONArray jsonArray"+l+" = jsonObject+"+(l-1)+".getJSONArray(\""+key+"\");";
		//				else
		//					jsonObject = "JSONObject jsonObject"+l+" = jsonObject"+(l-1)+".getJSONObject(\""+key+"\");";
		//
		//				if(!root.children.containsKey(keys[i])) {
		//					TreeNode tmp = new TreeNode(key, vt, colIndex);
		//					root.children.put(keys[i], tmp);
		//					tmp.jsonObject = jsonObject;
		//				}
		//				root = root.children.get(keys[i]);
		//			}
		//			colIndex++;
		//		}
		//		StringBuilder source = new StringBuilder();
		//		for(String rk : roots.keySet()) {
		//			TreeNode tn = roots.get(rk);
		//			String codes = tn.getCode(0);
		//			source.append(codes);
		//			source.append("\n");
		//		}
		//
		//		javaJSONTemplate = javaJSONTemplate.replace(code, source.toString());
		return javaJSONTemplate;
	}

	private void appendStringCondition(StringBuilder sb, String key, String document) {
		sb.append(" ").append(document);
		sb.append(".HasMember(\"" + key + "\")");
		//sb.append(" && ");
		//sb.append("!").append(document);
		//sb.append("[\"" + key + "\"].IsNull()");
	}

	private void appendArrayItemCondition(StringBuilder sb, String key, String document) {
		sb.append(" ").append(document);
		sb.append(".Size() > ").append(key);
	}

	private String getJSONKeyValue(String jsonObject, String key, Types.ValueType vt) {
		String code = jsonObject + ".get";
		switch(vt) {
			case INT32:
				code += "Int(" + key + ")";
				break;
			case INT64:
				code += "Long(" + key + ")";
				break;
			case FP64:
			case FP32:
				code += "Double(" + key + ")";
				break;
			case STRING:
				code += "String(" + key + ")";
				break;
			case BOOLEAN:
				code += "Boolean(" + key + ")";
				break;
			default:
				throw new RuntimeException("Format not supported!");
		}
		return code;
	}

	private String getJSONArrayValue(String jsonArray, Integer index, Types.ValueType vt) {
		String code = jsonArray + ".get";
		switch(vt) {
			case INT32:
				code += "Int(" + index + ")";
				break;
			case INT64:
				code += "Long(" + index + ")";
				break;
			case FP64:
			case FP32:
				code += "Double(" + index + ")";
				break;
			case STRING:
				code += "String(" + index + ")";
				break;
			case BOOLEAN:
				code += "Boolean(" + index + ")";
				break;
			default:
				throw new RuntimeException("Format not supported!");
		}
		return code;
	}

	private String getRandomName(String base) {
		Random r = new Random();
		int low = 0;
		int high = 100000000;
		int result = r.nextInt(high - low) + low;

		return base + "_" + result;
	}

	private void appendItem(StringBuilder sb, String jsonObject, String key, int colIndex, Types.ValueType vt) {
		sb.append("if(" + jsonObject + ".has(" + key + ")) { \n");
		sb.append("dest.set(row, " + colIndex + "," + getJSONKeyValue(jsonObject, key, vt) + "); \n");
		sb.append("}\n");
	}

	private class Trie {
		private TrieNode root;

		Trie() {
			root = new TrieNode();
		}

		void insert(String condition, int colIndex) {
			TrieNode current = root;
			String[] conditionLevels = condition.split("\\.");
			for(String cl : conditionLevels) {
				current = current.getChildren().computeIfAbsent(cl, c -> new TrieNode());
			}
			current.setEndOfCondition(true);
			current.setColIndex(colIndex);
		}
	}

	private class TrieNode {
		private final Map<String, TrieNode> children = new HashMap<>();
		private boolean endOfCondition;
		private int colIndex;
		private Types.ValueType valueType;
		private String key;
		private String parentKey;

		private Pair<String, String> getCode(String jsonObject){
			StringBuilder sbSource = new StringBuilder();
			StringBuilder sbHeader = new StringBuilder();
			if(endOfCondition){
				if(!isNumeric(key)){
					sbSource.append("if(" + jsonObject + ".has(" + key + "))");
					sbSource.append("dest.set(row, " + colIndex + "," + getJSONKeyValue(jsonObject, key, valueType) + "); \n");
				}
				else{
					sbSource.append("if("+jsonObject+".get("+colIndex+") !=null) \n");
					sbSource.append("dest.set(row, " + colIndex + "," + getJSONArrayValue(jsonObject, colIndex, valueType) + "); \n");
				}
			}
			else {
				Set<String> childrenKeys = children.keySet();

				// Array:
				if(isNumeric(childrenKeys.iterator().next())){
					int minIndex = 0;
					int maxIndex = 0;
					String setName = getRandomName("set");
					sbHeader.append("Set<Integer> "+setName+" = Set.of(");
					for(String k : childrenKeys) {
						int index = Integer.parseInt(k);
						sbHeader.append(index).append(",");
						minIndex = Math.min(index, minIndex);
						maxIndex = Math.max(maxIndex, index);
					}
					maxIndex++;
					sbHeader.deleteCharAt(sbHeader.length()-1);
					sbHeader.append(");");

					String listSize = getRandomName("listSize");
					String jsonArray = getRandomName("jsonArray");
					sbSource.append("if("+jsonObject+".has("+key+")){ \n");
					sbSource.append("JSONArray "+ jsonArray+"= "+jsonObject+".getJSONArray(\""+key+"\");");



					sbSource.append("int "+ listSize+" = Math.min("+ jsonArray+".length(), "+maxIndex+");");
					String loopIndex = getRandomName("i");
					sbSource.append("for(int "+loopIndex+"="+ minIndex+"; "+loopIndex+"<"+listSize+";"+loopIndex+"++){\n");
					sbSource.append("if("+setName+".contains("+loopIndex+")){\n");
					Pair<String, String> pairInnerLoop = children.get(childrenKeys.iterator().next()).getCode(jsonArray+"");
					sbSource.append(pairInnerLoop.getValue()).append("\n");

					sbSource.append("}\n");
					sbSource.append("}\n");
					sbSource.append("}\n");

					sbSource.append(" int " + listSize + " = 0; \n");
					//sbSource.append(listSize + " = " + currDocument + ".Size(); \n");
					//					sbSource.append("if(" + listSize + " > " + maxIndex + ") ")
					//						.append(listSize + " = " + maxIndex + ";\n");
					//					sbSource.append(
					//						"for (SizeType l" + level + " = " + minIndex + "; l" + level + " < " + listSize + "; l" + level + "++) {\n");
					//					sbSource.append("if( " + setName + ".find(l" + level + ") != " + setName + ".end()){ \n");
					//					level++;
				}
				//Object:
				else {
					for(String k: childrenKeys) {
						String randomName = getRandomName("jsonObject");
						sbSource.append("JSONObject "+randomName+" ="+jsonObject+".getJSONObject(\""+k+"\");");
						//children.get(k).getCode(randomName, sbSource);
					}
				}
			}

			return null;
		}


		Map<String, TrieNode> getChildren() {
			return children;
		}

		public boolean isEndOfCondition() {
			return endOfCondition;
		}

		public void setEndOfCondition(boolean endOfCondition) {
			this.endOfCondition = endOfCondition;
		}

		public int getColIndex() {
			return colIndex;
		}

		public void setColIndex(int colIndex) {
			this.colIndex = colIndex;
		}

		public Types.ValueType getValueType() {
			return valueType;
		}

		public void setValueType(Types.ValueType valueType) {
			this.valueType = valueType;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getParentKey() {
			return parentKey;
		}

		public void setParentKey(String parentKey) {
			this.parentKey = parentKey;
		}
	}

	private class TreeNode {
		private Types.ValueType valueType;
		private String key;
		private String jsonObject;
		private int colIndex;

		private HashMap<String, TreeNode> children = new HashMap<>();

		public TreeNode(String key, Types.ValueType valueType, int colIndex) {
			this.key = key;
			this.valueType = valueType;
			this.colIndex = colIndex;
		}

		public String getCode(int level) {
			StringBuilder source = new StringBuilder();

			if(children.size() == 0) {
				if(isNumeric(key)) {
					//					sbSource.append("colValue->push_back(getActualValue(" + document + "[l"+(level-1)+"]," + valueType + "));\n");
					//					sbSource.append("col->push_back(index);\n");
					//					sbSource.append("	index++; \n");
				}
				else
					appendItem(source, jsonObject, key, colIndex, valueType);
			}
			//			else {
			//				Set<String> ks = children.keySet();
			//				if(isNumeric(ks.iterator().next())) {
			//
			//					String currDocument = document + "[\"" + key + "\"]";
			//					sbSource.append("if(");
			//					appendStringCondition(sbSource, key, document);
			//					sbSource.append(" && ");
			//					appendArrayItemCondition(sbSource, "0", currDocument);
			//					sbSource.append("){ \n");
			//					String setName = getRandomName("indexSet");
			//					sbHeader.append("set <SizeType> " + setName + " = {");
			//					int minIndex = 0;
			//					int maxIndex = 0;
			//					for(String k : ks) {
			//						int index = Integer.parseInt(k);
			//						sbHeader.append(index).append(",");
			//						minIndex = Math.min(index, minIndex);
			//						maxIndex = Math.max(maxIndex, index);
			//					}
			//					maxIndex++;
			//					sbHeader.deleteCharAt(sbHeader.length() - 1);
			//					sbHeader.append("}; \n");
			//					String listSize = getRandomName("listSize");
			//					sbHeader.append(" int " + listSize + " = 0; \n");
			//					sbSource.append(listSize + " = " + currDocument + ".Size(); \n");
			//					sbSource.append("if(" + listSize + " > " + maxIndex + ") ")
			//						.append(listSize + " = " + maxIndex + ";\n");
			//					sbSource.append(
			//						"for (SizeType l" + level + " = " + minIndex + "; l" + level + " < " + listSize + "; l" + level + "++) {\n");
			//					sbSource.append("if( " + setName + ".find(l" + level + ") != " + setName + ".end()){ \n");
			//					level++;
			//
			//					Pair<String, String> pairCodes = children.get(ks.iterator().next()).getCode(level);
			//					sbSource.append(pairCodes.getKey());
			//					sbHeader.append(pairCodes.getValue());
			//
			//					sbSource.append("}\n");
			//					sbSource.append("}\n");
			//					sbSource.append("}\n");
			//
			//				}
			//				else {
			//					for(String s : ks) {
			//						TreeNode tn = children.get(s);
			//						if(tn.children.size() > 0) {
			//							sbSource.append("if(");
			//							appendStringCondition(sbSource, tn.key, tn.document);
			//							sbSource.append("){\n");
			//
			//							Pair<String, String> pairCodes = children.get(s).getCode(level);
			//							sbSource.append(pairCodes.getKey()).append("\n");
			//							sbHeader.append(pairCodes.getValue()).append("\n");
			//
			//							sbSource.append("\n}\n");
			//						}
			//						else {
			//							Pair<String, String> pairCodes = children.get(s).getCode(level);
			//							sbSource.append(pairCodes.getKey()).append("\n");
			//							sbHeader.append(pairCodes.getValue()).append("\n");
			//						}
			//					}
			//				}
			//			}
			return source.toString();
		}

		private String getRandomName(String base) {
			Random r = new Random();
			int low = 0;
			int high = 100000000;
			int result = r.nextInt(high - low) + low;

			return base + "_" + result;
		}
	}
}

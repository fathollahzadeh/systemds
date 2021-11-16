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

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.iogen.CustomProperties;
import org.apache.sysds.runtime.iogen.template.TemplateBase;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TemplateJavaJSON extends TemplateBase {

	private String code = "%code%";
	private String prop = "%prop%";
	private String javaJSONTemplate;

	public TemplateJavaJSON(CustomProperties _props, String className) {
		super(_props);
		javaJSONTemplate = "import org.apache.hadoop.io.LongWritable ; \n" +
		"import org.apache.hadoop.io.Text ; \n" +
		"import org.apache.hadoop.mapred.InputFormat ; \n" +
		"import org.apache.hadoop.mapred.InputSplit ; \n" +
		"import org.apache.hadoop.mapred.JobConf ; \n" +
		"import org.apache.hadoop.mapred.RecordReader ; \n" +
		"import org.apache.hadoop.mapred.Reporter ; \n" +
		"import org.apache.sysds.common.Types ; \n" +
		"import org.apache.sysds.runtime.io.IOUtilFunctions ; \n" +
		"import org.apache.sysds.runtime.iogen.CustomProperties ; \n" +
		"import org.apache.sysds.runtime.iogen.FrameGenerateReader ; \n" +
		"import org.apache.sysds.runtime.matrix.data.FrameBlock ; \n" +
		"import org.apache.wink.json4j.JSONObject ; \n" +
		"import org.apache.wink.json4j.JSONArray; \n" +
		"import java.io.IOException ; \n" +
		"public class "+className+" extends FrameGenerateReader { \n" +
		"	public "+className+"() { \n" +
				prop+
		"	} \n" +
		"	@Override protected int readFrameFromInputSplit(InputSplit split, InputFormat<LongWritable, Text> informat,\n" +
		"		JobConf job, FrameBlock dest, Types.ValueType[] schema, String[] names, long rlen, long clen, int rl, \n" +
		"		boolean first) throws IOException { \n" +
		"		// create record reader \n" +
		"		RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, job, Reporter.NULL) ; \n" +
		"		LongWritable key = new LongWritable() ; \n" +
		"		Text value = new Text() ; \n" +
		"		int row = rl ; \n" +
		"		try { \n"+
		"			while(reader.next(key, value)) { \n"+
		"				JSONObject jsonObject= new JSONObject(value.toString()) ; \n"+
		code+
		"				row++ ; \n"+
		"			} \n"+
		"		} \n"+
		"		catch(Exception e) { \n"+
		"			throw new RuntimeException(e) ; \n"+
		"		} \n"+
		"		finally { \n"+
		"			IOUtilFunctions.closeSilently(reader) ; \n"+
		"		} \n"+
		"		return row ; \n"+
		"	} \n"+
		"}\n";
	}

	public String getFrameReaderCode() {

		String[] colKeys = _props.getColKeys();
		Types.ValueType[] schema = _props.getSchema();

		String propCode = "_props = new CustomProperties(null,null); \n";
		javaJSONTemplate = javaJSONTemplate.replace(prop,propCode);

		Trie trie = new Trie();
		int colIndex = 0;
		for(String ck : colKeys) {
			trie.insert(ck, colIndex, schema[colIndex]);
			colIndex++;
		}

		trie.root.children.keySet();
		StringBuilder sb = new StringBuilder();
		for(String rk : trie.root.children.keySet()) {
			TrieNode tn = trie.root.children.get(rk);
			tn.getCode("jsonObject", sb, "");
			sb.append("\n");
		}
		javaJSONTemplate = javaJSONTemplate.replace(code, sb.toString());
		return javaJSONTemplate;
	}

	private String getJSONKeyValue(String jsonObject, String key, Types.ValueType vt) {
		String code = jsonObject + ".get";
		switch(vt) {
			case INT32:
				code += "Int(\"" + key + "\")";
				break;
			case INT64:
				code += "Long(\"" + key + "\")";
				break;
			case FP64:
			case FP32:
				code += "Double(\"" + key + "\")";
				break;
			case STRING:
				code += "String(\"" + key + "\")";
				break;
			case BOOLEAN:
				code += "Boolean(\"" + key + "\")";
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

	private class Trie {
		private TrieNode root;

		Trie() {
			root = new TrieNode(-1, "ROOT");
		}

		void insert(String condition, int colIndex, Types.ValueType valueType) {
			TrieNode current = root;
			String[] conditionLevels = condition.split("\\.");
			for(String cl : conditionLevels) {
				current = current.getChildren().computeIfAbsent(cl, c -> new TrieNode(colIndex, cl));
			}
			current.setEndOfCondition(true);
			current.setValueType(valueType);
		}
	}

	private class TrieNode {
		private final Map<String, TrieNode> children = new HashMap<>();
		private boolean endOfCondition;
		private int colIndex;
		private Types.ValueType valueType;
		private String key;

		public TrieNode(int colIndex, String key) {
			this.colIndex = colIndex;
			this.key = key;
		}

		private void getCode(String jsonObject, StringBuilder sbSource, String listSize) {
			if(endOfCondition) {
				if(!isNumeric(key)) {
					sbSource.append("if(" + jsonObject + ".has(\"" + key + "\")) \n");
					sbSource.append(
						"dest.set(row, " + colIndex + "," + getJSONKeyValue(jsonObject, key, valueType) + "); \n");
				}
				else {
					int arrIndex = Integer.parseInt(key);
					sbSource.append("if(" + listSize + " >" + arrIndex + ") \n");
					sbSource.append("dest.set(row, " + colIndex + "," + getJSONArrayValue(jsonObject, arrIndex, valueType) + "); \n");
				}
			}
			else {
				Set<String> childrenKeys = children.keySet();
				if(isNumeric(key)) {
					String randomName = getRandomName("jsonObject");
					sbSource.append("if("+listSize+" > "+ key+") {\n");
					sbSource.append("JSONObject " + randomName + " = (JSONObject) " + jsonObject + ".get("+key+"); \n");
					//Array
					if(isNumeric(childrenKeys.iterator().next())) {
						for(String k : childrenKeys) {
							children.get(k).getCode(randomName, sbSource, randomName);
						}
						sbSource.append("}\n");
					}
					//Object:
					else {
						for(String k : childrenKeys) {
							children.get(k).getCode(randomName, sbSource,"");
						}
						sbSource.append("}\n");
					}
				}

				// Array:
				else if(isNumeric(childrenKeys.iterator().next())) {
					String jsonArray = getRandomName("jsonArray");
					sbSource.append("if(" + jsonObject + ".has(\"" + key + "\")){ \n");
					sbSource.append("JSONArray " + jsonArray + "= " + jsonObject + ".getJSONArray(\"" + key + "\");\n");

					String ls = getRandomName("listSize");
					sbSource.append("int "+ls+" = "+ jsonArray+".length(); \n");

					for(String k : childrenKeys) {
						children.get(k).getCode(jsonArray, sbSource, ls);
					}
					sbSource.append("}\n");
				}
				//Object:
				else {
					String randomName = getRandomName("jsonObject");
					sbSource.append("if(" + jsonObject + ".has(\"" + key + "\")){ \n");
					sbSource.append(
						"JSONObject " + randomName + " = " + jsonObject + ".getJSONObject(\"" + key + "\");\n");
					for(String k : childrenKeys) {
						children.get(k).getCode(randomName, sbSource, "");
					}
					sbSource.append("}\n");
				}
			}
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
	}
}

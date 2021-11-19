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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class CodeGenTrieNode {

	public final Map<String, CodeGenTrieNode> children = new HashMap<>();
	private boolean endOfCondition;
	private int colIndex;
	private Types.ValueType valueType;
	private String key;

	public CodeGenTrieNode(int colIndex, String key) {
		this.colIndex = colIndex;
		this.key = key;
	}

	private String getRandomName(String base) {
		Random r = new Random();
		int low = 0;
		int high = 100000000;
		int result = r.nextInt(high - low) + low;

		return base + "_" + result;
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

	private String getJSONArrayValue(String jsonArray, String index, Types.ValueType vt) {
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

	private boolean isNumeric(String str) {
		try {
			Integer.parseInt(str);
			return true;
		}
		catch(NumberFormatException e) {
			return false;
		}
	}

	public void getCPPRapidJSONCode(String jsonObject, StringBuilder sbSource, String listSize) {

		if(endOfCondition) {
			if(!isNumeric(key)) {
				sbSource.append("	if (" + jsonObject + ".HasMember(\"" + key + "\")){ \n");
				sbSource.append("		colValue->push_back(getActualValue(" + jsonObject + "[\"" + key + "\"]," + valueType + ")); \n");
			}
			else {
				sbSource.append("	if(" + listSize + " > " + key + ") { \n");
				sbSource.append("		colValue->push_back(getActualValue(" + jsonObject + "[" + key + "]," + valueType + ")); \n");
			}

			sbSource.append("		col->push_back("+colIndex+"); \n");
			sbSource.append("} \n");
		}
		else {
			Set<String> childrenKeys = children.keySet();
			if(isNumeric(key)) {
				sbSource.append("if(" + listSize + " > " + key + ") {\n");
				jsonObject = jsonObject+"[" + key + "]";

				for(String k : childrenKeys) {
					children.get(k).getCPPRapidJSONCode(jsonObject, sbSource, "");
				}
				sbSource.append("} \n");
			}
			// Array:
			else if(isNumeric(childrenKeys.iterator().next())) {
				sbSource.append("if (" + jsonObject + ".HasMember(\"" + key + "\")){ \n");
				String ls = getRandomName("listSize");
				sbSource.append("int " + ls + " = " + jsonObject+"[\""+key+"\"].Size(); \n");
				for(String k : childrenKeys) {
					children.get(k).getCPPRapidJSONCode(jsonObject+"[\""+key+"\"]", sbSource, ls);
				}
				sbSource.append("}\n");
			}
			//Object:
			else {
				sbSource.append("if (" + jsonObject + ".HasMember(\"" + key + "\")){ \n");
				jsonObject = jsonObject+"[\"" + key +"\"]";
				for(String k : childrenKeys) {
					children.get(k).getCPPRapidJSONCode(jsonObject, sbSource, "");
				}
				sbSource.append("}\n");
			}
		}
	}

	public void getJavaJSONCode(String jsonObject, StringBuilder sbSource, String listSize) {
		if(endOfCondition) {
			if(!isNumeric(key)) {
				sbSource.append("if(" + jsonObject + ".has(\"" + key + "\")) \n");
				sbSource.append(
					"dest.set(row, " + colIndex + "," + getJSONKeyValue(jsonObject, key, valueType) + "); \n");
			}
			else {
				sbSource.append("if(" + listSize + " >" + key + ") \n");
				sbSource.append(
					"dest.set(row, " + colIndex + "," + getJSONArrayValue(jsonObject, key, valueType) + "); \n");
			}
		}
		else {
			Set<String> childrenKeys = children.keySet();
			if(isNumeric(key)) {
				String randomName = getRandomName("jsonObject");
				sbSource.append("if(" + listSize + " > " + key + ") {\n");
				sbSource.append("JSONObject " + randomName + " = (JSONObject) " + jsonObject + ".get(" + key + "); \n");
				//Array
				if(isNumeric(childrenKeys.iterator().next())) {
					for(String k : childrenKeys) {
						children.get(k).getJavaJSONCode(randomName, sbSource, randomName);
					}
					sbSource.append("}\n");
				}
				//Object:
				else {
					for(String k : childrenKeys) {
						children.get(k).getJavaJSONCode(randomName, sbSource, "");
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
				sbSource.append("int " + ls + " = " + jsonArray + ".length(); \n");

				for(String k : childrenKeys) {
					children.get(k).getJavaJSONCode(jsonArray, sbSource, ls);
				}
				sbSource.append("}\n");
			}
			//Object:
			else {
				String randomName = getRandomName("jsonObject");
				sbSource.append("if(" + jsonObject + ".has(\"" + key + "\")){ \n");
				sbSource.append("JSONObject " + randomName + " = " + jsonObject + ".getJSONObject(\"" + key + "\");\n");
				for(String k : childrenKeys) {
					children.get(k).getJavaJSONCode(randomName, sbSource, "");
				}
				sbSource.append("}\n");
			}
		}
	}

	Map<String, CodeGenTrieNode> getChildren() {
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

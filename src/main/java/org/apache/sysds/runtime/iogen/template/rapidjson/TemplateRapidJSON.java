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

package org.apache.sysds.runtime.iogen.template.rapidjson;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.iogen.CustomProperties;
import org.apache.sysds.runtime.matrix.data.Pair;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;

public class TemplateRapidJSON extends TemplateBaseRapidJSON {

	public TemplateRapidJSON(CustomProperties _props) {
		super(_props);
	}

	@Override public String getFrameReaderCode(String cppClassName, String sourceFileName, String headerFileName) {

		String[] colKeys = _props.getColKeys();
		Types.ValueType[] schema = _props.getSchema();

		HashMap<String, TreeNode> roots = new HashMap<>();
		int index = 0;
		for(String ck : colKeys) {

			Types.ValueType vt = schema[index++];
			String[] keys = ck.split("\\.");
			StringBuilder document = new StringBuilder("d");

			TreeNode root;
			if(!roots.containsKey(keys[0])) {
				root = new TreeNode(keys[0], vt);
				root.document = document.toString();
				roots.put(keys[0], root);
			}
			else
				root = roots.get(keys[0]);

			int l = 0;
			for(int i = 1; i < keys.length; i++) {
				if(isNumeric(keys[i - 1]))
					document.append("[l" + (l++) + "]");
				else
					document.append("[\"" + keys[i - 1] + "\"]");

				if(!root.children.containsKey(keys[i])) {
					TreeNode tmp = new TreeNode(keys[i], vt);
					root.children.put(keys[i], tmp);
					tmp.document = document.toString();
				}
				root = root.children.get(keys[i]);
			}
		}
		StringBuilder sbSource = new StringBuilder();
		StringBuilder sbHeader = new StringBuilder();
		for(String rk : roots.keySet()) {
			TreeNode tn = roots.get(rk);
			Pair<String, String> pairCodes = tn.getCode(0);
			sbSource.append(pairCodes.getKey());
			sbSource.append("\n");
			sbHeader.append(pairCodes.getValue());
			sbHeader.append("\n");
		}

		sourceTemplate = sourceTemplate.replace(code, sbSource.toString());
		sourceTemplate = sourceTemplate.replace(className, cppClassName);

		headerTemplate = headerTemplate.replace(code, sbHeader.toString());
		headerTemplate = headerTemplate.replace(className, cppClassName);

		saveCode(sourceFileName, sourceTemplate);
		saveCode(headerFileName, headerTemplate);

		return sourceTemplate;
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

	private void appendItem(StringBuilder sb, String document, String key, Types.ValueType vt) {
		sb.append(
			"	if (" + document + ".HasMember(\"" + key + "\")){ \n"); //&& !"+document+"[\"" + key + "\"].IsNull()
		sb.append(
			"colValue->push_back(getActualValue(" + document + "[\"" + key + "\"]," + vt + "));\n col->push_back(index);\n } \n");
		sb.append("	index++; \n");
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

	private class TreeNode {
		private Types.ValueType valueType;
		private String key;
		private String document;

		private HashMap<String, TreeNode> children = new HashMap<>();

		public TreeNode(String key, Types.ValueType valueType) {
			this.key = key;
			this.valueType = valueType;
		}

		public Pair<String, String> getCode(int level) {
			StringBuilder sbSource = new StringBuilder();
			StringBuilder sbHeader = new StringBuilder();

			if(children.size() == 0) {
				if(isNumeric(key)) {
					sbSource.append("colValue->push_back(getActualValue(" + document + "[l"+(level-1)+"]," + valueType + "));\n");
					sbSource.append("col->push_back(index);\n");
					sbSource.append("	index++; \n");
				}
				else
					appendItem(sbSource, document, key, valueType);
			}
			else {
				Set<String> ks = children.keySet();
				if(isNumeric(ks.iterator().next())) {

					String currDocument = document + "[\"" + key + "\"]";
					sbSource.append("if(");
					appendStringCondition(sbSource, key, document);
					sbSource.append(" && ");
					appendArrayItemCondition(sbSource, "0", currDocument);
					sbSource.append("){ \n");
					String setName = getRandomName("indexSet");
					sbHeader.append("set <SizeType> " + setName + " = {");
					int minIndex = 0;
					int maxIndex = 0;
					for(String k : ks) {
						int index = Integer.parseInt(k);
						sbHeader.append(index).append(",");
						minIndex = Math.min(index, minIndex);
						maxIndex = Math.max(maxIndex, index);
					}
					maxIndex++;
					sbHeader.deleteCharAt(sbHeader.length() - 1);
					sbHeader.append("}; \n");
					String listSize = getRandomName("listSize");
					sbHeader.append(" int " + listSize + " = 0; \n");
					sbSource.append(listSize + " = " + currDocument + ".Size(); \n");
					sbSource.append("if(" + listSize + " > " + maxIndex + ") ")
						.append(listSize + " = " + maxIndex + ";\n");
					sbSource.append(
						"for (SizeType l" + level + " = " + minIndex + "; l" + level + " < " + listSize + "; l" + level + "++) {\n");
					sbSource.append("if( " + setName + ".find(l" + level + ") != " + setName + ".end()){ \n");
					level++;

					Pair<String, String> pairCodes = children.get(ks.iterator().next()).getCode(level);
					sbSource.append(pairCodes.getKey());
					sbHeader.append(pairCodes.getValue());

					sbSource.append("}\n");
					sbSource.append("}\n");
					sbSource.append("}\n");

				}
				else {
					for(String s : ks) {
						TreeNode tn = children.get(s);
						if(tn.children.size() > 0) {
							sbSource.append("if(");
							appendStringCondition(sbSource, tn.key, tn.document);
							sbSource.append("){\n");

							Pair<String, String> pairCodes = children.get(s).getCode(level);
							sbSource.append(pairCodes.getKey()).append("\n");
							sbHeader.append(pairCodes.getValue()).append("\n");

							sbSource.append("\n}\n");
						}
						else {
							Pair<String, String> pairCodes = children.get(s).getCode(level);
							sbSource.append(pairCodes.getKey()).append("\n");
							sbHeader.append(pairCodes.getValue()).append("\n");
						}
					}
				}
			}
			return new Pair<>(sbSource.toString(), sbHeader.toString());
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

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
import org.apache.sysds.runtime.iogen.CustomProperties;

public class TemplateCPPRapidJSON extends TemplateBase {

	private String headerTemplate;
	private String sourceTemplate;
	private String code = "%code%";
	private String className = "GIOClassName";
	private String sourceFileName;
	private String headerFileName;

	public TemplateCPPRapidJSON(CustomProperties _props, String className, String sourceFileName,
		String headerFileName) {
		super(_props);
		this.sourceFileName = sourceFileName;
		this.headerFileName = headerFileName;

		headerTemplate = "#ifndef RAPIDJSONCPP_"+className+"_H \n"+
			"#define RAPIDJSONCPP_"+className+"_H \n"+
			"#include \"FrameReader.h\" \n"+
			"#include <set> \n" +
			"class "+className+" : public FrameReader  { \n"+
			"private: \n"+
			code + "\n"+
			"public: \n"+
			className+"(); \n"+
			"virtual ~"+className+"(); \n"+
			"void getJSONValues(vector<long> *col, vector<ItemObject*> *colValue, Document &d) override; \n"+
			"}; \n"+
			"#endif //RAPIDJSONCPP_"+className+"_H \n";
		sourceTemplate = "#include \""+className+".h\" \n"+
			className+"::"+className+"() {} \n"+
			className+"::~"+className+"() {} \n"+
			"void "+className+"::getJSONValues(vector<long> *col, vector<ItemObject*> *colValue, Document &d) { \n"+
			" int index = 0; \n"+
			code+"\n"+
			"} \n";
	}

	@Override public String getFrameReaderCode() {

		String[] colKeys = _props.getColKeys();
		Types.ValueType[] schema = _props.getSchema();

		CodeGenTrie trie = new CodeGenTrie();
		int colIndex = 0;
		for(String ck : colKeys) {
			trie.insert(ck, colIndex, schema[colIndex]);
			colIndex++;
		}

		StringBuilder sb = new StringBuilder();
		for(String rk : trie.root.children.keySet()) {
			CodeGenTrieNode tn = trie.root.children.get(rk);
			tn.getCPPRapidJSONCode("d", sb, "");
			sb.append("\n");
		}
		sourceTemplate = sourceTemplate.replace(code, sb.toString());
		sourceTemplate = sourceTemplate.replace(className, className);

		headerTemplate = headerTemplate.replace(code, "");
		headerTemplate = headerTemplate.replace(className, className);

		saveCode(sourceFileName, sourceTemplate);
		saveCode(headerFileName, headerTemplate);

		return sourceTemplate;
	}
}

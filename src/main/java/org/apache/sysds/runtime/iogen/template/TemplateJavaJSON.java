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
				prop +
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
						code +
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

	@Override
	public String getFrameReaderCode() {

		String[] colKeys = _props.getColKeys();
		Types.ValueType[] schema = _props.getSchema();

		String propCode = "_props = new CustomProperties(null,null); \n";
		javaJSONTemplate = javaJSONTemplate.replace(prop,propCode);

		CodeGenTrie trie = new CodeGenTrie();
		int colIndex = 0;
		for(String ck : colKeys) {
			trie.insert(ck, colIndex, schema[colIndex]);
			colIndex++;
		}

		StringBuilder sb = new StringBuilder();
		for(String rk : trie.root.children.keySet()) {
			CodeGenTrieNode tn = trie.root.children.get(rk);
			tn.getJavaJSONCode("jsonObject", sb, "");
			sb.append("\n");
		}
		javaJSONTemplate = javaJSONTemplate.replace(code, sb.toString());
		return javaJSONTemplate;
	}
}

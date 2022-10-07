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

package org.apache.sysds.test.functions.iogen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.gson.Gson;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.io.FrameReader;
import org.apache.sysds.runtime.iogen.EXP.Util;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.wink.json4j.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FrameSingleRowFlatTest extends GenerateReaderFrameTest {

	private final static String TEST_NAME = "FrameSingleRowFlatTest";

	@Override
	protected String getTestName() {
		return TEST_NAME;
	}


	// CSV: Frame
	// 1. dataset contain INT32 values
	@Test
	public void test1() {
		sampleRaw = "1,2,3,4,5\n" + "6,7,8,9,10\n" + "11,12,13,14,15";
		data = new String[][] {{"1", "2"}, {"6", "7"}, {"11", "12"}};
		schema = new Types.ValueType[] {Types.ValueType.INT32, Types.ValueType.INT32};
		runGenerateReaderTest(false);
	}

	// 2. dataset contain different value types
	@Test
	public void test2() {
		sampleRaw = "1,2,a,b,c\n" + "6,7,aa,bb,cc\n" + "11,12,13,14,15";
		data = new String[][] {{"1", "2"}, {"6", "7"}, {"11", "12"}};
		schema = new Types.ValueType[] {Types.ValueType.INT32, Types.ValueType.INT32};
		runGenerateReaderTest(false);
	}

	@Test
	public void test3() {
		sampleRaw = "1,2,a,b,c\n" + "6,7,aa,bb,cc\n" + "11,12,13,14,15";
		data = new String[][] {{"1", "2"}, {"6", "7"}, {"11", "12"}};
		schema = new Types.ValueType[] {Types.ValueType.INT32, Types.ValueType.STRING};
		runGenerateReaderTest(false);
	}

	@Test
	public void test4() {
		sampleRaw = "1,2,a,b,c\n" + "6,7,aa,bb,cc\n" + "11,12,13,14,15";
		data = new String[][] {{"1", "2", "b"}, {"6", "7", "bb"}, {"11", "12", "14"}};
		schema = new Types.ValueType[] {Types.ValueType.INT32, Types.ValueType.INT32, Types.ValueType.STRING};
		runGenerateReaderTest(false);
	}

	@Test
	public void test5() {
		sampleRaw = "1,2,a,b,c\n" + "6,7,aa,bb,cc\n" + "11,12,13,14,15";
		data = new String[][] {{"1", "2", "b"}, {"6", "7", "bb"}, {"11", "12", "14"}};
		schema = new Types.ValueType[] {Types.ValueType.INT32, Types.ValueType.FP64, Types.ValueType.STRING};
		runGenerateReaderTest(false);
	}

	// CSV with empty values
	@Test
	public void test6() {
		sampleRaw = "1,2,a,,c\n" + "6,,aa,bb,cc\n" + ",12,13,14,15";
		data = new String[][] {{"1", "2", ""}, {"6", "0", "bb"}, {"0", "12", "14"}};
		schema = new Types.ValueType[] {Types.ValueType.INT32, Types.ValueType.INT32, Types.ValueType.STRING};
		runGenerateReaderTest(false);
	}

	@Test
	public void test77() throws JsonProcessingException {
		String str="<student> \n"
			+"<name>1</name>\n"+
			"</student>";

		XmlMapper mapper = new XmlMapper();
		JsonNode root = mapper.readTree(str);
		Map<String, String> map = new HashMap<>();
		addKeys("", root, map, new ArrayList<>());
		Gson gson = new Gson();
		System.out.println(gson.toJson(map));
	}

	private void addKeys(String currentPath, JsonNode jsonNode, Map<String, String> map, List<Integer> suffix) {
		if (jsonNode.isObject()) {
			ObjectNode objectNode = (ObjectNode) jsonNode;
			Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
			String pathPrefix = currentPath.isEmpty() ? "" : currentPath + "/";

			while (iter.hasNext()) {
				Map.Entry<String, JsonNode> entry = iter.next();
				addKeys(pathPrefix + entry.getKey(), entry.getValue(), map, suffix);
			}
		} else if (jsonNode.isArray()) {
			ArrayNode arrayNode = (ArrayNode) jsonNode;
			for (int i = 0; i < arrayNode.size(); i++) {
				suffix.add(i + 1);
				addKeys(currentPath+"-"+i, arrayNode.get(i), map, suffix);
				if (i + 1 <arrayNode.size()){
					suffix.remove(suffix.size() - 1);
				}
			}

		} else if (jsonNode.isValueNode()) {
			if (currentPath.contains("/") && !currentPath.contains("-")) {
				for (int i = 0; i < suffix.size(); i++) {
					currentPath += "/" + suffix.get(i);
				}
				suffix = new ArrayList<>();
			}
			ValueNode valueNode = (ValueNode) jsonNode;
			map.put("/"+currentPath, valueNode.asText());
		}
	}

	@Test
	public void test66() throws Exception {
		String sampleRawFileName;
		String sampleFrameFileName;
		String sampleRawDelimiter;
		String schemaFileName;
		String dataFileName;
		boolean parallel;
		long rows = -1;

		String base = "/home/sfathollahzadeh/Documents/GitHub/papers/2023-sigmod-GIO/Experiments/data";
		sampleRawFileName = base +"/autolead-xml/sample-autolead-xml200.raw"; //System.getProperty("sampleRawFileName");
		sampleFrameFileName = base+"/autolead-xml/sample-autolead-xml200.frame";//System.getProperty("sampleFrameFileName");
		sampleRawDelimiter = "\t";
		schemaFileName = base+"/autolead-xml/autolead-xml.schema";//System.getProperty("schemaFileName");
		dataFileName = base +"/autolead-xml.dat";
		parallel = false;//Boolean.parseBoolean(System.getProperty("parallel"));
		Util util = new Util();

		// read and parse mtd file
		String mtdFileName = dataFileName + ".mtd";
		try {
			String mtd = util.readEntireTextFile(mtdFileName);
			mtd = mtd.replace("\n", "").replace("\r", "");
			mtd = mtd.toLowerCase().trim();
			JSONObject jsonObject = new JSONObject(mtd);
			if (jsonObject.containsKey("rows")) rows = jsonObject.getLong("rows");
		} catch (Exception exception) {}

		Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
		int ncols = sampleSchema.length;

		String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
		FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
		String sampleRaw = util.readEntireTextFile(sampleRawFileName);
		GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame, parallel);
		FrameReader fr = gr.getReader();
		FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

		for(int i =0; i< frameBlock.getNumRows(); i++){
			System.out.println(i+">>>  ");
			for(int j=0; j<frameBlock.getNumColumns(); j++){
				System.out.print(j+":"+frameBlock.get(i,j).toString()+"\t");
			}
			System.out.println();
		}
	}
}

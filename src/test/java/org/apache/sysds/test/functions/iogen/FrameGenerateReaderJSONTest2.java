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

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.io.FrameReader;
import org.apache.sysds.runtime.io.FrameReaderTextCSV;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.apache.sysds.runtime.iogen.exp.Util;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.util.UtilFunctions;
import org.apache.sysds.test.functions.iogen.objects.ComplexObject1;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class FrameGenerateReaderJSONTest2 extends GenerateReaderFrameTest {

	private final static String TEST_NAME = "MatrixGenerateReaderCSVTest";

	@Override
	protected String getTestName() {
		return TEST_NAME;
	}

	@Test
	public void test1() throws Exception {
		String sample_raw_fileName="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/IMDB/imdb_sample_150.raw";
		String sample_frame_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/IMDB/imdb_sample_150.frame";
		int sample_fram_nrows=150;
		String sample_delimiter="\t";
		String schema_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/IMDB/imdb.schema";
		String data_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/IMDB/imdb.data";

		Util util = new Util();
		Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
		int ncols = sampleSchema.length;
		FrameBlock sampleFrame = new FrameBlock(sampleSchema,util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

		String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
		GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);
		FrameReader fr = gr.getReader();
		FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, sampleSchema, -1, ncols);

	}

	@Test
	public void test2() throws Exception {
		String sample_raw_fileName="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/TPCH/tpch_sample_150.raw";
		String sample_frame_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/TPCH/tpch_sample_150.frame";
		int sample_fram_nrows=150;
		String sample_delimiter="|";
		String schema_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/TPCH/tpch.schema";
		String data_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/TPCH/tpch.data";

		Util util = new Util();
		Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
		int ncols = sampleSchema.length;

		FileFormatPropertiesCSV csvpro = new FileFormatPropertiesCSV(false, sample_delimiter, false);
		HashSet<String> nas = new HashSet<>();
		nas.add("NULL");
		csvpro.setNAStrings(nas);
		FrameReaderTextCSV csv = new FrameReaderTextCSV(csvpro);
		FrameBlock sampleFrame = csv.readFrameFromHDFS(sample_frame_file_name, sampleSchema, sample_fram_nrows, ncols);

		String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
		GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);
		FrameReader fr = gr.getReader();
		FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, sampleSchema, -1, ncols);

	}
}

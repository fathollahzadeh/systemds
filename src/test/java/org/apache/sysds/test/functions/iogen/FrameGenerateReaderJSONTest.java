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
import org.apache.sysds.runtime.io.FrameReader;
import org.apache.sysds.runtime.iogen.GenerateReader;

import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.junit.Test;

public class FrameGenerateReaderJSONTest extends GenerateReaderFrameTest {

	private final static String TEST_NAME = "FrameGenerateReaderJSONTest";

	@Override
	protected String getTestName() {
		return TEST_NAME;
	}

	@Test
	public void test1() throws Exception {

		String[] percentS = {"10", "20", "30","40","50","60","70","80","90","100"};
		for(int i=9;i<10;i++) {
			String[] percentF = new String[] {"0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"};
			String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/sample_100_" + percentF[i] + ".raw";
			String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/sample_100_" + percentF[i] + ".frame";
			int sample_fram_nrows = 100;
			String sample_delimiter = "\t";
			String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/aminer_" + percentF[i] + ".schema";
			String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/aminer.data";

			Util util = new Util();
			Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
			int ncols = sampleSchema.length;
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

			String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);

			String baseFileName = "/home/sfathollahzadeh/Documents/GitHub/papers/2022-icde-gIO/experiments/benchmark/RapidJSONCPP/src/at/tugraz/";
			String sourceFileName = baseFileName + "source/FrameReaderGIO_" + percentS[i] + ".cpp";
			String headerFileName = baseFileName + "header/FrameReaderGIO_" + percentS[i] + ".h";

			//gr.getReader("FrameReaderGIO_" + percentS[i], sourceFileName, headerFileName);
			//FrameReader fr = gr.getReader();
			//FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, sampleSchema, -1, ncols);

		}
	}


	@Test
	public void test2() throws Exception {

		String[] percentS = {"10", "20", "30","40","50","60","70","80","90","100"};
		for(int i=0;i<10;i++) {
			String[] percentF = new String[] {"0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"};
			String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_" + percentF[i] + ".raw";
			String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_" + percentF[i] + ".frame";
			int sample_fram_nrows = 100;
			String sample_delimiter = "\t";
			String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb_" + percentF[i] + ".schema";
			String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb.data";

			Util util = new Util();
			Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
			int ncols = sampleSchema.length;
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

			String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);

			String baseFileName = "/home/sfathollahzadeh/Documents/GitHub/papers/2022-icde-gIO/experiments/benchmark/RapidJSONCPP/src/at/tugraz/";
			String sourceFileName = baseFileName + "source/FrameReaderGIO_" + percentS[i] + ".cpp";
			String headerFileName = baseFileName + "header/FrameReaderGIO_" + percentS[i] + ".h";

			//gr.getReader("FrameReaderGIO_" + percentS[i], sourceFileName, headerFileName);
			//FrameReader fr = gr.getReader();
			//FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, sampleSchema, -1, ncols);
			//break;
		}
	}


	@Test
	public void test3() throws Exception {

		String[] percentS = {"10", "20", "30","40","50","60","70","80","90","100"};
		for(int i=9;i<10;i++) {
			String[] percentF = new String[] {"0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"};
			String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_" + percentF[i] + ".raw";
			String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_" + percentF[i] + ".frame";
			int sample_fram_nrows = 100;
			String sample_delimiter = "\t";
			String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb_" + percentF[i] + ".schema";
			String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb.data";

			Util util = new Util();
			Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
			int ncols = sampleSchema.length;
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

			String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);

			//String s = gr.getReaderJavaJSON();
			int a= 100;

			//FrameReader fr = gr.getReader();
			//FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, sampleSchema, -1, ncols);
			//break;
		}
	}


	@Test
	public void test4() throws Exception {

		String[] percentS = {"10", "20", "30","40","50","60","70","80","90","100"};
		for(int i=9;i<10;i++) {
			String[] percentF = new String[] {"0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"};
			String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/sample_100_" + percentF[i] + ".raw";
			String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/sample_100_" + percentF[i] + ".frame";
			int sample_fram_nrows = 100;
			String sample_delimiter = "\t";
			String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/aminer_" + percentF[i] + ".schema";
			String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/aminer.data";

			Util util = new Util();
			Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
			int ncols = sampleSchema.length;
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

			String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);

			//String s = gr.getReaderJavaJSON();
			int a= 100;

			//FrameReader fr = gr.getReader();
			//FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, sampleSchema, -1, ncols);
			//break;
		}
	}

	@Test
	public void test5() throws Exception {


			String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/sample_100_1.0.raw";
			String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/sample_100_1.0.frame";
			int sample_fram_nrows = 100;
			String sample_delimiter = "\t";
			String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/aminer_1.0.schema";
			String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/aminer/aminer.data";

			Util util = new Util();
			Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
			int ncols = sampleSchema.length;
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

			String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);
			gr.getReader(true);

			//FrameReader fr = new GIOFrameReader(gr.getProperties());
			//FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, gr.getProperties().getSchema(), -1, gr.getProperties().getSchema().length);
			//break;
		}

	@Test
	public void test6() throws Exception {

		String[] percentS = {"10", "20", "30","40","50","60","70","80","90","100"};
		for(int i=9;i<10;i++) {
			String[] percentF = new String[] {"0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"};
			String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_" + percentF[i] + ".raw";
			String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_" + percentF[i] + ".frame";
			int sample_fram_nrows = 100;
			String sample_delimiter = "\t";
			String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb_" + percentF[i] + ".schema";
			String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb.data";

			Util util = new Util();
			Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
			int ncols = sampleSchema.length;
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

			String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);

			//String s = gr.getReaderJavaJSON();
			int a= 100;

			//FrameReader fr = gr.getReader();
			//FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, sampleSchema, -1, ncols);
			//break;
		}
	}

	@Test
	public void test7() throws Exception {


		String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_1.0.raw";
		String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_1.0.frame";
		int sample_fram_nrows = 100;
		String sample_delimiter = "\t";
		String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb_1.0.schema";
		String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb.data";

		Util util = new Util();
		Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
		int ncols = sampleSchema.length;
		FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

		String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
		GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);
		gr.getReader(true);

		//FrameReader fr = new GIOFrameReader(gr.getProperties());
		//FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, gr.getProperties().getSchema(), -1, gr.getProperties().getSchema().length);
		//break;
	}

	@Test
	public void test8() throws Exception {


		String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_0.1.raw";
		String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_0.1.frame";
		int sample_fram_nrows = 100;
		String sample_delimiter = "\t";
		String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb_0.1.schema";
		String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb.data";

		Util util = new Util();
		Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
		int ncols = sampleSchema.length;
		FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

		String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
		GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);

		FrameReader fr = gr.getReader(true);
		//FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, sampleSchema, -1, ncols);

		//gr.getReader();

		//FrameReader fr = new GIOFrameReader(gr.getProperties());
		double tmpTime = System.nanoTime();
		FrameBlock frameBlock = fr.readFrameFromHDFS(data_file_name, gr.getProperties().getSchema(), -1, gr.getProperties().getSchema().length);
		double readTime = (System.nanoTime() - tmpTime) / 1000000000.0;
		System.out.println(readTime);
		//break;
	}

	@Test
	public void test9() throws Exception {
		String[] percentS = {"10", "20", "30","40","50","60","70","80","90","100"};
		for(int i=0;i<10;i++) {
			String[] percentF = new String[] {"0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"};
			String sample_raw_fileName = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_" + percentF[i] + ".raw";
			String sample_frame_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/sample_100_" + percentF[i] + ".frame";
			int sample_fram_nrows = 100;
			String sample_delimiter = "\t";
			String schema_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb_" + percentF[i] + ".schema";
			String data_file_name = "/media/sfathollahzadeh/Windows1/saeedData/NestedDatasets/imdb/imdb.data";

			Util util = new Util();
			Types.ValueType[] sampleSchema = util.getSchema(schema_file_name);
			int ncols = sampleSchema.length;
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sample_frame_file_name, sample_fram_nrows, ncols, sample_delimiter));

			String sampleRaw = util.readEntireTextFile(sample_raw_fileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);

			String baseFileName = "/home/sfathollahzadeh/Documents/GitHub/papers/2022-vldb-GIO/Experiments/benchmark/RapidJSONCPP/src/at/tugraz/";
			String sourceFileName = baseFileName + "source/FrameReaderGIO_" + percentS[i] + ".cpp";
			String headerFileName = baseFileName + "header/FrameReaderGIO_" + percentS[i] + ".h";

			gr.getReaderRapidJSON("FrameReaderGIO_"+percentS[i], sourceFileName, headerFileName);
		}

	}

}

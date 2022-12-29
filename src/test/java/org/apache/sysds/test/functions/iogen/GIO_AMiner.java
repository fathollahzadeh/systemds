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

import com.google.gson.Gson;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.frame.data.FrameBlock;
import org.apache.sysds.runtime.io.FrameReader;
import org.apache.sysds.runtime.iogen.CustomProperties;
import org.apache.sysds.runtime.iogen.EXP.Util;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.junit.Test;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class GIO_AMiner extends HL7ReaderFrameTest {

	private final static String TEST_NAME = "GIO_AMiner";

	@Override protected String getTestName() {
		return TEST_NAME;
	}


	@Test public void test1() throws Exception {

		String[] projections = new String[]{"F1", "F2", "F3", "F4", "F5", "F6", "F7"};//{"Q1", "Q2", "Q3", "Q4"};  //{"Q1", "Q2", "Q3", "Q4", "F1", "F2", "F3", "F4", "F5", "F6", "F7"};
		Integer[] counts = new Integer[]{200}; //{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/aminer-author-json";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			for(String p : projections) {
				System.out.println("P >> "+ p);
				String sampleRawFileName = rootPath + "/" + p + "/sample-aminer-author-json"+c+".raw";
				String sampleFrameFileName = rootPath + "/" + p + "/sample-aminer-author-json"+c+".frame";
				String schemaFileName = rootPath + "/" + p + "/aminer-author-json.schema";
				String dataFileName = sampleRawFileName;

				Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
				int ncols = sampleSchema.length;
				String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
				FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
				String sampleRaw = util.readEntireTextFile(sampleRawFileName);
				GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
					parallel);
				FrameReader fr = gr.getReader();
				FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

				for(int i=0; i< 5; i++) {
					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
						System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
					}
					System.out.println();
				}
				System.out.println(c+"++++++++++++++++++++++++++++++++++");
			}
		}
	}

	@Test public void test2() throws Exception {

		String[] projections = new String[]{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10"};  //{"Q1", "Q2", "Q3", "Q4", "F1", "F2", "F3", "F4", "F5", "F6", "F7"};
		Integer[] counts = new Integer[]{200}; //{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/aminer-paper-json";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			for(String p : projections) {
				System.out.println("P="+p);
				String sampleRawFileName = rootPath + "/" + p + "/sample-aminer-paper-json"+c+".raw";
				String sampleFrameFileName = rootPath + "/" + p + "/sample-aminer-paper-json"+c+".frame";
				String schemaFileName = rootPath + "/" + p + "/aminer-paper-json.schema";
				String dataFileName = sampleRawFileName;

				Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
				int ncols = sampleSchema.length;
				String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
				FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
				String sampleRaw = util.readEntireTextFile(sampleRawFileName);
				GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
					parallel);
				FrameReader fr = gr.getReader();
				FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

				for(int i=0; i< 5; i++) {
					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
						System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
					}
					System.out.println();
				}
				System.out.println(c+"+++++++++++++++++++++++++");
			}
		}
	}

	@Test public void test3() throws Exception {

		String[] projections = new String[]{"F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10",
			"F11", "F12", "F13", "F14", "F15", "F16", "F17", "F18", "F19", "F20"};  //{"Q1", "Q2", "Q3", "Q4", "F1", "F2", "F3", "F4", "F5", "F6", "F7"};
		Integer[] counts = new Integer[]{200}; //{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/yelp-json";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			for(String p : projections) {
				String sampleRawFileName = rootPath + "/" + p + "/sample-yelp-json"+c+".raw";
				String sampleFrameFileName = rootPath + "/" + p + "/sample-yelp-json"+c+".frame";
				String schemaFileName = rootPath + "/" + p + "/yelp-json.schema";
				String dataFileName = sampleRawFileName;

				Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
				int ncols = sampleSchema.length;
				String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
				FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
				String sampleRaw = util.readEntireTextFile(sampleRawFileName);
				GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
					parallel);
				FrameReader fr = gr.getReader();
				FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

				for(int i=0; i< frameBlock.getNumRows(); i++) {
					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
						System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
					}
					System.out.println();
				}
			}
		}
	}

	@Test public void test4() throws Exception {

		String[] projections = new String[]{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9"};  //{"Q1", "Q2", "Q3", "Q4", "F1", "F2", "F3", "F4", "F5", "F6", "F7"};
		Integer[] counts = new Integer[]{200};//{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/yelp-csv";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			for(String p : projections) {
				System.out.println("P="+p);
				String sampleRawFileName = rootPath + "/" + p + "/sample-yelp-csv"+c+".raw";
				String sampleFrameFileName = rootPath + "/" + p + "/sample-yelp-csv"+c+".frame";
				String schemaFileName = rootPath + "/" + p + "/yelp-csv.schema";
				String dataFileName = sampleRawFileName;

				Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
				int ncols = sampleSchema.length;
				String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
				FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
				String sampleRaw = util.readEntireTextFile(sampleRawFileName);
				GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
					parallel);
				FrameReader fr = gr.getReader();
				FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

				for(int i=0; i< 5; i++) {
					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
						System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
					}
					System.out.println();
				}
				System.out.println(c+"++++++++++++++++++++++++");
			}
		}
	}

	@Test public void test5() throws Exception {

		String[] projections = new String[]{"F1", "F2", "F3", "F4", "F5", "F6", "F7"};  //{"Q1", "Q2", "Q3", "Q4", "F1", "F2", "F3", "F4", "F5", "F6", "F7"};
		Integer[] counts = new Integer[]{200};//{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/aminer-author";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			for(String p : projections) {
				String sampleRawFileName = rootPath + "/" + p + "/sample-aminer-author"+c+".raw";
				String sampleFrameFileName = rootPath + "/" + p + "/sample-aminer-author"+c+".frame";
				String schemaFileName = rootPath + "/" + p + "/aminer-author.schema";
				String dataFileName = sampleRawFileName;

				Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
				int ncols = sampleSchema.length;
				String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
				FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
				String sampleRaw = util.readEntireTextFile(sampleRawFileName);
				GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
					parallel);
				FrameReader fr = gr.getReader();
				FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

				for(int i=0; i< 5; i++) {
					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
						System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
					}
					System.out.println();
				}

				System.out.println("+++++++++++++++++++++++++++++++++++++++++");
			}
		}
	}

	@Test public void test6() throws Exception {

		String[] projections = new String[]{"Q1", "Q2", "Q3", "Q4"};  //{"Q1", "Q2", "Q3", "Q4", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10"};
		Integer[] counts = new Integer[]{200};//{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/aminer-paper";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			for(String p : projections) {
				String sampleRawFileName = rootPath + "/" + p + "/sample-aminer-paper"+c+".raw";
				String sampleFrameFileName = rootPath + "/" + p + "/sample-aminer-paper"+c+".frame";
				String schemaFileName = rootPath + "/" + p + "/aminer-paper.schema";
				String dataFileName = sampleRawFileName;

				Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
				int ncols = sampleSchema.length;
				String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
				FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
				String sampleRaw = util.readEntireTextFile(sampleRawFileName);
				GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
					parallel);
				FrameReader fr = gr.getReader();
				FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

				for(int i=0; i< 5; i++) {
					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
						System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
					}
					System.out.println();
				}
				System.out.println(c+"++++++++++++++++++++++++++++++++++++");
			}
		}
	}

	@Test public void test7() throws Exception {

		Integer[] counts = new Integer[]{200};//{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/aminer-author";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
				String sampleRawFileName = rootPath + "/sample-aminer-author"+c+".raw";
				String sampleFrameFileName = rootPath + "/sample-aminer-author"+c+".frame";
				String schemaFileName = rootPath + "/aminer-author.schema";
				String dataFileName = sampleRawFileName;

				Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
				int ncols = sampleSchema.length;
				String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
				FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
				String sampleRaw = util.readEntireTextFile(sampleRawFileName);
				GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
					parallel);
				FrameReader fr = gr.getReader();
				FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

				for(int i=0; i< frameBlock.getNumRows(); i++) {
					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
						System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
					}
					System.out.println();
				}
		}
	}

	@Test public void test8() throws Exception {

		Integer[] counts = new Integer[]{200};//{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/aminer-paper";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			String sampleRawFileName = rootPath + "/sample-aminer-paper"+c+".raw";
			String sampleFrameFileName = rootPath + "/sample-aminer-paper"+c+".frame";
			String schemaFileName = rootPath + "/aminer-paper.schema";
			String dataFileName = sampleRawFileName;

			Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
			int ncols = sampleSchema.length;
			String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
				parallel);
			FrameReader fr = gr.getReader();
			FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

			for(int i=0; i< frameBlock.getNumRows(); i++) {
				for(int j = 0; j < frameBlock.getNumColumns(); j++) {
					System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
				}
				System.out.println();
			}
		}
	}


	@Test public void test9() throws Exception {

		String[] projections = new String[] {"F10"}; //{"F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10"};
		Integer[] counts = new Integer[]{200} ;//{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/ReWasteF-csv";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			for(String p : projections) {
				String sampleRawFileName = rootPath + "/" + p + "/sample-ReWasteF-csv"+c+".raw";
				String sampleFrameFileName = rootPath + "/" + p + "/sample-ReWasteF-csv"+c+".frame";
				String schemaFileName = rootPath + "/" + p + "/ReWasteF-csv.schema";
				String dataFileName = sampleRawFileName;

				Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
				int ncols = sampleSchema.length;
				String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
				FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
				String sampleRaw = util.readEntireTextFile(sampleRawFileName);
				GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
					parallel);
				FrameReader fr = gr.getReader(); //new GIOReader_3039648(gr.getProperties());//
				FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

//				for(int i=0; i< frameBlock.getNumRows(); i++) {
//					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
//						if(!frameBlock.get(i, j).equals(sampleFrame.get(i, j)))
//							System.out.println("["+i+","+j+"]>>"+frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
//					}
//					//System.out.println();
//				}
				System.out.println(c+"++++++++++++++++++++++++++++++++++++");
			}
		}
	}

	@Test public void test10() throws Exception {

		Integer[] counts = new Integer[]{200};//{200,300,400,500,600,700,800,900,1000};

		String rootPath="/home/saeed/Documents/tmp/Examples/autolead-xml";
		String sampleRawDelimiter="\t";
		boolean parallel=false;
		long rows = -1;
		Util util = new Util();

		for(Integer c: counts) {
			String sampleRawFileName = rootPath + "/sample-autolead-xml"+c+".raw";
			String sampleFrameFileName = rootPath + "/sample-autolead-xml"+c+".frame";
			String schemaFileName = rootPath + "/autolead-xml.schema";
			String dataFileName = sampleRawFileName;

			Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
			int ncols = sampleSchema.length;
			String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
				parallel);
			FrameReader fr = gr.getReader();
			FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

			for(int i=0; i< 10; i++) {
				for(int j = 0; j < frameBlock.getNumColumns(); j++) {
					System.out.print(frameBlock.get(i, j) + "[" + sampleFrame.get(i, j) + "], ");
				}
				System.out.println();
			}
		}
	}

	@Test public void test11() throws Exception {

		int blklen = (int)Math.ceil((double)1015 / (16*16));
		for(int i=0; i<16; i++) {
			System.out.print(i+" >> ");
			for(int j=0; j<16 && j*16*blklen+(i)*blklen < 1015; j++){
				int begin = j*16*blklen+i*blklen;//j*blklen+i*16;
				int end = (int)Math.min(j*16*blklen+(i+1)*blklen, 1015);
				System.out.print("["+begin+","+end+"] , ");
			}
			System.out.println();
		}
	}
}

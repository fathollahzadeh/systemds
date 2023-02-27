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
import org.apache.sysds.runtime.frame.data.FrameBlock;
import org.apache.sysds.runtime.io.FrameReader;
import org.apache.sysds.runtime.io.MatrixReader;
import org.apache.sysds.runtime.iogen.EXP.Util;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.junit.Test;

import java.util.ArrayList;

public class GIO_Matrix extends HL7ReaderFrameTest {

	private final static String TEST_NAME = "GIO_AMiner";

	@Override
	protected String getTestName() {
		return TEST_NAME;
	}

	@Test
	public void test1() throws Exception {

		String[] projections = new String[] {"Q1", "Q2"};  //{"Q1", "Q2", "Q3", "Q4", "F1", "F2", "F3", "F4", "F5", "F6", "F7"};
		Integer[] counts = new Integer[] {1000,2000,3000,4000,5000,6000,7000,8000,9000,10000};

		String rootPath = "/home/saeed/Documents/tmp/Examples/message-hl7";
		String sampleRawDelimiter = "\t";
		boolean parallel = false;
		long rows = -1;
		Util util = new Util();

		for(Integer c : counts) {
			for(String p : projections) {
				System.out.println("P >> " + p);
				String sampleRawFileName = rootPath + "/" + p + "/sample-message-hl7" + c + ".raw";
				String sampleFrameFileName = rootPath + "/" + p + "/sample-message-hl7" + c + ".frame";
				String schemaFileName = rootPath + "/" + p + "/message-hl7.schema";
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

				for(int i = 0; i < 5; i++) {
					for(int j = 0; j < frameBlock.getNumColumns(); j++) {
						//System.out.print("["+j+"]"+sampleFrame.get(i,j)+",  ");
						//if(!frameBlock.get(i, j).equals(sampleFrame.get(i,j))){
							System.out.print("++["+j+"] >> "+  frameBlock.get(i, j) + "[" + sampleFrame.get(i,j) + "], "); //sampleFrame.get(i, j)
						//}
						//System.out.print(frameBlock.get(i, j).equals() + "[" + sampleFrame.get(i,j) + "], "); //sampleFrame.get(i, j)
					}
					System.out.println();
				}
				System.out.println(c + "++++++++++++++++++++++++++++++++++");
			}
		}
	}

	@Test
	public void test2() throws Exception {

		Integer[] counts = new Integer[] {200};//{200,300,400,500,600,700,800,900,1000};

		String rootPath = "/home/saeed/Documents/tmp/Examples/message-hl7";
		String sampleRawDelimiter = "\t";
		boolean parallel = false;
		long rows = -1;
		Util util = new Util();

		for(Integer c : counts) {
			String sampleRawFileName = rootPath + "/sample-message-hl7" + c + ".raw";
			String sampleFrameFileName = rootPath + "/sample-message-hl7" + c + ".frame";
			String schemaFileName = rootPath + "/message-hl7.schema";
			String dataFileName = sampleRawFileName;

			Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
			int ncols = sampleSchema.length;
			String[][] sampleFrameStrings = util.loadFrameData(sampleFrameFileName, sampleRawDelimiter, ncols);
			FrameBlock sampleFrame = new FrameBlock(sampleSchema, sampleFrameStrings);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);
			GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame,
				parallel);
			FrameReader fr = gr.getReader(); //new GIOReader_45338777(gr.getProperties()); //gr.getReader();
			FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, rows, sampleSchema.length);

			for(int i = 0; i < frameBlock.getNumRows(); i++) {
				for(int j = 0; j < frameBlock.getNumColumns(); j++) {
					System.out.print(frameBlock.get(i, j) + ", ");
				}
				System.out.println();
			}
		}
	}

	@Test
	public void test3() throws Exception {

		ArrayList<String> projections = new ArrayList<>();
		for(int i = 28; i <= 28; i++)
			projections.add("F" + i);
		int c = 200;
		String rootPath = "/home/saeed/Documents/tmp/Examples/aaa/higgs-csv/";
		String sampleRawDelimiter = "\t";
		boolean parallel = false;
		long rows = -1;
		Util util = new Util();
		for(String p : projections) {
			System.out.println("P >> " + p);
			String sampleRawFileName = rootPath + "/" + p + "/sample-higgs-csv200.raw";
			String sampleMatrixFileName = rootPath + "/" + p + "/sample-higgs-csv200.matrix";
			String dataFileName = sampleRawFileName;

			MatrixBlock sampleMB = util.loadMatrixData(sampleMatrixFileName, sampleRawDelimiter);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);

			GenerateReader.GenerateReaderMatrix gr = new GenerateReader.GenerateReaderMatrix(sampleRaw, sampleMB, parallel);
			MatrixReader matrixReader = gr.getReader();
			MatrixBlock matrixBlock = matrixReader.readMatrixFromHDFS(dataFileName, rows, sampleMB.getNumColumns(), -1, -1);

			for(int i = 0; i <5; i++) { // matrixBlock.getNumRows()
				for(int j = 0; j < matrixBlock.getNumColumns(); j++) {
					System.out.print(matrixBlock.getValue(i, j) + "[" + sampleMB.getValue(i,j) + "], "); //sampleFrame.get(i, j)
				}
				System.out.println();
			}
			System.out.println(c + "++++++++++++++++++++++++++++++++++");
		}
	}

	@Test
	public void test4() throws Exception {

		ArrayList<String> projections = new ArrayList<>();
		for(int i = 28; i <= 28; i++)
			projections.add("F" + i);
		int c = 200;
		String rootPath = "/home/saeed/Documents/tmp/Examples/aaa/mnist8m-libsvm/";
		String sampleRawDelimiter = "\t";
		boolean parallel = false;
		long rows = -1;
		Util util = new Util();
		for(String p : projections) {
			System.out.println("P >> " + p);
			String sampleRawFileName = rootPath + "/" + p + "/sample-mnist8m-libsvm200.raw";
			String sampleMatrixFileName = rootPath + "/" + p + "/sample-mnist8m-libsvm200.matrix";
			String dataFileName = sampleRawFileName;

			MatrixBlock sampleMB = util.loadMatrixData(sampleMatrixFileName, sampleRawDelimiter);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);

			GenerateReader.GenerateReaderMatrix gr = new GenerateReader.GenerateReaderMatrix(sampleRaw, sampleMB, parallel);
			MatrixReader matrixReader = gr.getReader();
			MatrixBlock matrixBlock = matrixReader.readMatrixFromHDFS(dataFileName, rows, sampleMB.getNumColumns(), -1, -1);

			for(int i = 0; i <5; i++) { // matrixBlock.getNumRows()
				for(int j = 0; j < matrixBlock.getNumColumns(); j++) {
					System.out.print(matrixBlock.getValue(i, j) + "[" + sampleMB.getValue(i,j) + "], "); //sampleFrame.get(i, j)
				}
				System.out.println();
			}
			System.out.println(c + "++++++++++++++++++++++++++++++++++");
		}
	}

	@Test
	public void test5() throws Exception {

		ArrayList<String> projections = new ArrayList<>();
		for(int i = 7; i <= 7; i++)
			projections.add("F" + i);
		int c = 1000;
		String rootPath = "/home/saeed/Documents/tmp/Examples/queen-mm/";
		String sampleRawDelimiter = "\t";
		boolean parallel = true;
		long rows = 200;
		Util util = new Util();
		for(String p : projections) {
			System.out.println("P >> " + p);
			String sampleRawFileName = rootPath + "/" + p + "/sample-queen-mm200.raw";
			String sampleMatrixFileName = rootPath + "/" + p + "/sample-queen-mm200.matrix";
			String dataFileName = sampleRawFileName;

			MatrixBlock sampleMB = util.loadMatrixData(sampleMatrixFileName, sampleRawDelimiter);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);

			GenerateReader.GenerateReaderMatrix gr = new GenerateReader.GenerateReaderMatrix(sampleRaw, sampleMB, parallel);
			MatrixReader matrixReader = gr.getReader();//new GIOReader_64336800(gr.getProperties());//gr.getReader();
			MatrixBlock matrixBlock = matrixReader.readMatrixFromHDFS(dataFileName, rows, sampleMB.getNumColumns(), -1, -1);

			for(int i = 0; i <5; i++) { // matrixBlock.getNumRows()
				for(int j = 0; j < matrixBlock.getNumColumns(); j++) {
					System.out.print(matrixBlock.getValue(i, j) + "[" + sampleMB.getValue(i,j) + "], "); //sampleFrame.get(i, j)
				}
				System.out.println();
			}
			System.out.println(c + "++++++++++++++++++++++++++++++++++");
		}
	}

	@Test
	public void test6() throws Exception {

		ArrayList<String> projections = new ArrayList<>();
		for(int i = 1; i <= 28; i++)
			projections.add("F" + i);
		int c = 1000;
		String rootPath = "/home/saeed/Documents/tmp/Examples/aaa/higgs-csv/";
		String sampleRawDelimiter = "\t";
		boolean parallel = true;
		long rows = -1;
		Util util = new Util();
		for(String p : projections) {
			System.out.println("P >> " + p);
			String sampleRawFileName = rootPath + "/" + p + "/sample-higgs-csv200.raw";
			String sampleMatrixFileName = rootPath + "/" + p + "/sample-higgs-csv200.matrix";
			String dataFileName = sampleRawFileName;

			MatrixBlock sampleMB = util.loadMatrixData(sampleMatrixFileName, sampleRawDelimiter);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);

			GenerateReader.GenerateReaderMatrix gr = new GenerateReader.GenerateReaderMatrix(sampleRaw, sampleMB, parallel);
			MatrixReader matrixReader = gr.getReader();
			MatrixBlock matrixBlock = matrixReader.readMatrixFromHDFS(dataFileName, rows, sampleMB.getNumColumns(), -1, -1);

			for(int i = 0; i <5; i++) { // matrixBlock.getNumRows()
				for(int j = 0; j < matrixBlock.getNumColumns(); j++) {
					System.out.print(matrixBlock.getValue(i, j) + "[" + sampleMB.getValue(i,j) + "], "); //sampleFrame.get(i, j)
				}
				System.out.println();
			}
			System.out.println(c + "++++++++++++++++++++++++++++++++++");
		}
	}

	@Test
	public void test7() throws Exception {

		ArrayList<String> projections = new ArrayList<>();
		for(int i = 0; i <= 28; i++)
			projections.add("F" + i);
		int c = 200;
		String rootPath = "/home/saeed/Documents/tmp/Examples/aaa/mnist8m-libsvm/";
		String sampleRawDelimiter = "\t";
		boolean parallel = true;
		long rows = -1;
		Util util = new Util();
		for(String p : projections) {
			System.out.println("P >> " + p);
			String sampleRawFileName = rootPath + "/" + p + "/sample-mnist8m-libsvm200.raw";
			String sampleMatrixFileName = rootPath + "/" + p + "/sample-mnist8m-libsvm200.matrix";
			String dataFileName = sampleRawFileName;

			MatrixBlock sampleMB = util.loadMatrixData(sampleMatrixFileName, sampleRawDelimiter);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);

			GenerateReader.GenerateReaderMatrix gr = new GenerateReader.GenerateReaderMatrix(sampleRaw, sampleMB, parallel);
			MatrixReader matrixReader = gr.getReader();
			MatrixBlock matrixBlock = matrixReader.readMatrixFromHDFS(dataFileName, rows, sampleMB.getNumColumns(), -1, -1);

			for(int i = 0; i <5; i++) { // matrixBlock.getNumRows()
				for(int j = 0; j < matrixBlock.getNumColumns(); j++) {
					System.out.print(matrixBlock.getValue(i, j) + "[" + sampleMB.getValue(i,j) + "], "); //sampleFrame.get(i, j)
				}
				System.out.println();
			}
			System.out.println(c + "++++++++++++++++++++++++++++++++++");
		}
	}

	@Test
	public void test8() throws Exception {

		ArrayList<String> projections = new ArrayList<>();
		for(int i = 1; i <= 18; i++)
			projections.add("F" + i);
		int c = 200;
		String rootPath = "/home/saeed/Documents/tmp/Examples/aaa/susy-libsvm/";
		String sampleRawDelimiter = "\t";
		boolean parallel = true;
		long rows = -1;
		Util util = new Util();
		for(String p : projections) {
			System.out.println("P >> " + p);
			String sampleRawFileName = rootPath + "/" + p + "/sample-susy-libsvm200.raw";
			String sampleMatrixFileName = rootPath + "/" + p + "/sample-susy-libsvm200.matrix";
			String dataFileName = sampleRawFileName;

			MatrixBlock sampleMB = util.loadMatrixData(sampleMatrixFileName, sampleRawDelimiter);
			String sampleRaw = util.readEntireTextFile(sampleRawFileName);

			GenerateReader.GenerateReaderMatrix gr = new GenerateReader.GenerateReaderMatrix(sampleRaw, sampleMB, parallel);
			MatrixReader matrixReader = gr.getReader();
			MatrixBlock matrixBlock = matrixReader.readMatrixFromHDFS(dataFileName, rows, sampleMB.getNumColumns(), -1, -1);

			for(int i = 0; i <5; i++) { // matrixBlock.getNumRows()
				for(int j = 0; j < matrixBlock.getNumColumns(); j++) {
					System.out.print(matrixBlock.getValue(i, j) + "[" + sampleMB.getValue(i,j) + "], "); //sampleFrame.get(i, j)
				}
				System.out.println();
			}
			System.out.println(c + "++++++++++++++++++++++++++++++++++");
		}
	}
}

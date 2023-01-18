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

import org.apache.sysds.runtime.io.MatrixReader;
import org.apache.sysds.runtime.iogen.EXP.Util;
import org.apache.sysds.runtime.iogen.early.GenerateReader;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.junit.Test;

import java.util.ArrayList;

public class GIO_MatrixEarly extends HL7ReaderFrameTest {

	private final static String TEST_NAME = "GIO_AMiner";

	@Override
	protected String getTestName() {
		return TEST_NAME;
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

			GenerateReader.GenerateReaderMatrix gr = new GenerateReader.GenerateReaderMatrix(sampleRaw, sampleMB);
			MatrixReader matrixReader = gr.getReader();
			int ff = 100;

		}
	}

}

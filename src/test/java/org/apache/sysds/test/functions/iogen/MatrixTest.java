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

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.conf.CompilerConfig;
import org.apache.sysds.runtime.io.MatrixReader;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixValue;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public abstract class MatrixTest extends AutomatedTestBase {

	protected final static String TEST_DIR = "functions/iogen/";
	protected final static String TEST_CLASS_DIR = TEST_DIR + MatrixTest.class.getSimpleName() + "/";

	protected String sampleRaw;
	protected String sampleMatrixPath;
	protected String sampleMatrixFormat;
	protected String sep;
	protected String indSep;
	protected boolean parallel;
	protected int cols;


	protected abstract String getTestName();

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(getTestName(), new TestConfiguration(TEST_DIR, getTestName(), new String[] {"Y"}));
	}

	@SuppressWarnings("unused")
	protected void runMatrixTest() {

		Types.ExecMode oldPlatform = rtplatform;
		rtplatform = Types.ExecMode.SINGLE_NODE;

		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		boolean oldpar = CompilerConfig.FLAG_PARREADWRITE_TEXT;

		try {
			CompilerConfig.FLAG_PARREADWRITE_TEXT = parallel;
			TestConfiguration config = getTestConfiguration(getTestName());
			loadTestConfiguration(config);


			String HOME = SCRIPT_DIR + TEST_DIR;
			sampleRaw =  HOME + INPUT_DIR + sampleRaw;
			sampleMatrixPath = HOME + INPUT_DIR + sampleMatrixPath;

			fullDMLScriptName = HOME + getTestName() + ".dml";

			//$2, format=$3, sep=$4,indSep=$5, cols=$6, data_type="matrix"
			programArgs = new String[] {"-args", sampleRaw, sampleMatrixPath, sampleMatrixFormat, sep, indSep, cols+"", output("Y")};
			fullRScriptName = HOME + "ReadHDF5_Verify.R";
			runTest(true, false, null, -1);
//
//			HashMap<MatrixValue.CellIndex, Double> YR = readRMatrixFromExpectedDir("Y");
//			HashMap<MatrixValue.CellIndex, Double> YSYSTEMDS = readDMLMatrixFromOutputDir("Y");
//			TestUtils.compareMatrices(YR, YSYSTEMDS, eps, "YR", "YSYSTEMDS");

		}
		catch(Exception exception) {
			exception.printStackTrace();
		}
		finally {
			rtplatform = oldPlatform;
			CompilerConfig.FLAG_PARREADWRITE_TEXT = oldpar;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
}

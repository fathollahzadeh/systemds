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
import org.apache.sysds.runtime.io.FileFormatPropertiesHL7;
import org.junit.Test;

public class FrameHL7Test extends HL7ReaderFrameTest {

	private final static String TEST_NAME = "FrameHL7Test";

	@Override protected String getTestName() {
		return TEST_NAME;
	}


	@Test public void test1() {
		Types.ValueType[] schema = new Types.ValueType[109];
		String[] names = new String[109];
		for(int i = 0; i < 109; i++) {
			schema[i] = Types.ValueType.STRING;
			names[i] = "nam_" + i;
		}

		FileFormatPropertiesHL7 properties = new FileFormatPropertiesHL7(new int[0], 109);

		runGenerateReaderTest(false, schema, names, properties);
	}

	@Test public void test2() {
		Types.ValueType[] schema = new Types.ValueType[10];
		String[] names = new String[10];
		for(int i = 0; i < 10; i++) {
			schema[i] = Types.ValueType.STRING;
			names[i] = "nam_" + i;
		}

		FileFormatPropertiesHL7 properties = new FileFormatPropertiesHL7(new int[0], 10);

		runGenerateReaderTest(false, schema, names, properties);
	}

	@Test public void test3() {
		Types.ValueType[] schema = new Types.ValueType[5];
		String[] names = new String[5];
		for(int i = 0; i < 5; i++) {
			schema[i] = Types.ValueType.STRING;
			names[i] = "nam_" + i;
		}
		int[] list = {1,5,10,15,20};
		FileFormatPropertiesHL7 properties = new FileFormatPropertiesHL7(list, 10);
		runGenerateReaderTest(false, schema, names, properties);
	}

	@Test public void test4() {
		Types.ValueType[] schema = new Types.ValueType[109];
		String[] names = new String[109];
		for(int i = 0; i < 109; i++) {
			schema[i] = Types.ValueType.STRING;
			names[i] = "nam_" + i;
		}

		FileFormatPropertiesHL7 properties = new FileFormatPropertiesHL7(new int[0], 109);

		runGenerateReaderTest(true, schema, names, properties);
	}
}

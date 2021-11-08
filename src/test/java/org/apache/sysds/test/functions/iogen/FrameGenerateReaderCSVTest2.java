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
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.io.FrameReader;
import org.apache.sysds.runtime.io.FrameReaderTextCSV;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.apache.sysds.runtime.iogen.exp.Util;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.junit.Test;

import java.io.IOException;

public class FrameGenerateReaderCSVTest2 extends GenerateReaderFrameTest {

	private final static String TEST_NAME = "FrameGenerateReaderCSVTest";

	@Override
	protected String getTestName() {
		return TEST_NAME;
	}


	//time java -Dlog4j.configuration=file:/home/sfathollahzadeh/Documents/GitHub/systemds/scripts/perftest/conf/log4j.properties -Xms1g -Xmx15g -cp /home/sfathollahzadeh/Documents/GitHub/systemds/target/SystemDS.jar:/home/sfathollahzadeh/Documents/GitHub/systemds/target/lib/* org.apache.sysds.runtime.iogen.exp.GIOFrameExperimentHDFS /media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/sample_10_2000.raw /media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/sample_10_2000.frame 10 , /media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/csv.schema /media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/csv.data 1.0 csv /media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/LOG/benchmark/GIOFrameExperiment/csv1.csv

	@Test
	public void test1() throws Exception {
		String sampleRawFileName = "/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/sample_10_2000.raw";
		String sampleFrameFileName = "/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/sample_10_2000.frame";
		Integer sampleNRows = 10;
		String delimiter = ",";
		String schemaFileName = "/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/csv.schema";
		String dataFileName = "/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/csv.data";

		Float percent = 1.0f;
		String datasetName = "csv";
		String LOG_HOME ="/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/LOG/benchmark/GIOFrameExperiment/csv1.csv";

		Util util = new Util();
		Types.ValueType[] schema = util.getSchema(schemaFileName);
		int ncols = schema.length;


		FileFormatPropertiesCSV csvpro = new FileFormatPropertiesCSV(false, delimiter, false);
		FrameReaderTextCSV csv = new FrameReaderTextCSV(csvpro);
		FrameBlock sampleFrame = csv.readFrameFromHDFS(sampleFrameFileName, schema,sampleNRows,ncols);

		double tmpTime = System.nanoTime();
		String sampleRaw = util.readEntireTextFile(sampleRawFileName);
		GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);
		FrameReader fr = gr.getReader();
		double generateTime = (System.nanoTime() - tmpTime) / 1000000000.0;

		Gson gson=new Gson();
		System.out.println(gson.toJson(fr));

		tmpTime = System.nanoTime();
		FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, schema, -1, schema.length);
		double readTime = (System.nanoTime() - tmpTime) / 1000000000.0;

		String log= datasetName+","+ frameBlock.getNumRows()+","+ ncols+","+percent+","+ sampleNRows+","+ generateTime+","+readTime;
		util.addLog(LOG_HOME, log);
	}


}

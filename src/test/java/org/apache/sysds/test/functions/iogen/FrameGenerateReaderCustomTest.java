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
import org.junit.Test;

public class FrameGenerateReaderCustomTest extends GenerateReaderFrameTest {

	private final static String TEST_NAME = "FrameGenerateReaderCustomTest";

	@Override
	protected String getTestName() {
		return TEST_NAME;
	}

	private void extractSampleRawCSV(String separator) {
		int nrows = data.length;
		int ncols = data[0].length;
		StringBuilder sb = new StringBuilder();
		for(int r = 0; r < nrows; r++) {
			for(int c = 0; c < ncols; c++) {
				sb.append(data[r][c]);
				if(c != ncols - 1)
					sb.append(separator);
			}
			if(r != nrows - 1)
				sb.append("\n");
		}
		sampleRaw = sb.toString();
	}

	@Test
	public void test1() {
		sampleRaw = "#index 1083734\n" +
					"#* ArnetMiner: extraction and mining of academic social networks\n" +
					"#@ Jie Tang;Jing Zhang;Limin Yao;Juanzi Li;Li Zhang;Zhong Su\n" + //6: 2-7
					"#o Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;IBM, Beijing, China;IBM, Beijing, China\n" +
					"#t 2008\n" +
					"#c Proceedings of the 14th ACM SIGKDD international conference on Knowledge discovery and data mining\n" +
					"#% 197394\n" +
					"#% 220708\n" +
					"#% 280819\n" +
					"#% 387427\n" +
					"#% 464434\n" +
					"#% 643007\n" +
					"#% 722904\n" +
					"#% 760866\n" +
					"#% 766409\n" +
					"#% 769881\n" +
					"#% 769906\n" +
					"#% 788094\n" +
					"#% 805885\n" +
					"#% 809459\n" +
					"#% 817555\n" +
					"#% 874510\n" +
					"#% 879570\n" +
					"#% 879587\n" +
					"#% 939393\n" +
					"#% 956501\n" +
					"#% 989621\n" +
					"#% 1117023\n" +
					"#% 1250184\n" +
					"#! This paper addresses several key issues \n"+
			"#index 1083734\n" +
			"#* ArnetMiner: extraction and mining of academic social networks\n" +
			"#@ Jie Tang;Jing Zhang;Limin Yao;Juanzi Li;Li Zhang;Zhong Su\n" + //6: 2-7
			"#o Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;IBM, Beijing, China;IBM, Beijing, China\n" +
			"#t 2008\n" +
			"#c Proceedings of the 14th ACM SIGKDD international conference on Knowledge discovery and data mining\n" +
			"#% 197394\n" +
			"#% 220708\n" +
			"#% 280819\n" +
			"#% 387427\n" +
			"#% 464434\n" +
			"#% 643007\n" +
			"#% 722904\n" +
			"#% 760866\n" +
			"#% 766409\n" +
			"#% 769881\n" +
			"#% 769906\n" +
			"#% 788094\n" +
			"#% 805885\n" +
			"#% 809459\n" +
			"#% 817555\n" +
			"#% 874510\n" +
			"#% 879570\n" +
			"#% 879587\n" +
			"#% 939393\n" +
			"#% 956501\n" +
			"#% 989621\n" +
			"#% 1117023\n" +
			"#% 1250184\n" +
			"#! This paper addresses several key issues ";
		schema = new Types.ValueType[40];
		schema[0] = Types.ValueType.INT64;
		schema[1] = Types.ValueType.STRING;
		schema[2] = Types.ValueType.STRING;
		schema[3] = Types.ValueType.STRING;
		schema[4] = Types.ValueType.STRING;
		schema[5] = Types.ValueType.STRING;
		schema[6] = Types.ValueType.STRING;
		schema[7] = Types.ValueType.STRING;
		schema[8] = Types.ValueType.STRING;
		schema[9] = Types.ValueType.STRING;
		schema[10] = Types.ValueType.STRING;
		schema[11] = Types.ValueType.STRING;
		schema[12] = Types.ValueType.STRING;
		schema[13] = Types.ValueType.STRING;
		schema[14] = Types.ValueType.INT32;
		schema[15] = Types.ValueType.STRING;
		schema[16] = Types.ValueType.INT64;
		schema[17] = Types.ValueType.INT64;
		schema[18] = Types.ValueType.INT64;
		schema[19] = Types.ValueType.INT64;
		schema[20] = Types.ValueType.INT64;
		schema[21] = Types.ValueType.INT64;
		schema[22] = Types.ValueType.INT64;
		schema[23] = Types.ValueType.INT64;
		schema[24] = Types.ValueType.INT64;
		schema[25] = Types.ValueType.INT64;
		schema[26] = Types.ValueType.INT64;
		schema[27] = Types.ValueType.INT64;
		schema[28] = Types.ValueType.INT64;
		schema[29] = Types.ValueType.INT64;
		schema[30] = Types.ValueType.INT64;
		schema[31] = Types.ValueType.INT64;
		schema[32] = Types.ValueType.INT64;
		schema[33] = Types.ValueType.INT64;
		schema[34] = Types.ValueType.INT64;
		schema[35] = Types.ValueType.INT64;
		schema[36] = Types.ValueType.INT64;
		schema[37] = Types.ValueType.INT64;
		schema[38] = Types.ValueType.INT64;
		schema[39] = Types.ValueType.STRING;

		data = new String[2][40];
		data[0][0] = "1083734";
		data[0][1] = "ArnetMiner: extraction and mining of academic social networks";
		data[0][2] = "Jie Tang";
		data[0][3] = "Jing Zhang";
		data[0][4] = "Limin Yao";
		data[0][5] = "Juanzi Li";
		data[0][6] = "Li Zhang";
		data[0][7] = "Zhong Su";
		data[0][8] = "Tsinghua University, Beijing, China";
		data[0][9] = "Tsinghua University, Beijing, China";
		data[0][10] = "Tsinghua University, Beijing, China";
		data[0][11] = "Tsinghua University, Beijing, China";
		data[0][12] = "IBM, Beijing, China";
		data[0][13] = "IBM, Beijing, China";
		data[0][14] = "2008";
		data[0][15] = "Proceedings of the 14th ACM SIGKDD international conference on Knowledge discovery and data mining";
		data[0][16] = "197394";
		data[0][17] = "220708";
		data[0][18] = "280819";
		data[0][19] = "387427";
		data[0][20] = "464434";
		data[0][21] = "643007";
		data[0][22] = "722904";
		data[0][23] = "760866";
		data[0][24] = "766409";
		data[0][25] = "769881";
		data[0][26] = "769906";
		data[0][27] = "788094";
		data[0][28] = "805885";
		data[0][29] = "809459";
		data[0][30] = "817555";
		data[0][31] = "874510";
		data[0][32] = "879570";
		data[0][33] = "879587";
		data[0][34] = "939393";
		data[0][35] = "956501";
		data[0][36] = "989621";
		data[0][37] = "1117023";
		data[0][38] = "1250184";
		data[0][39] = "This paper addresses several key issues ";

		for(int i=0; i<data[0].length; i++)
			data[1][i] = data[0][i];

		names = new String[data[0].length];
		for(int i = 0; i < names.length; i++)
			names[i] = "C_" + i;
		runGenerateReaderTest();
	}

	@Test
	public void test2() {
		sampleRaw = "#index 1083734\n" +
			"#* ArnetMiner: extraction and mining of academic social networks\n" +
			"#@ Jie Tang;Jing Zhang;Limin Yao;Juanzi Li;Li Zhang;Zhong Su\n" + //6: 2-7
			"#o Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;IBM, Beijing, China;IBM, Beijing, China\n" +
			"#t 2008\n" +
			"#c Proceedings of the 14th ACM SIGKDD international conference on Knowledge discovery and data mining\n" +
			"#% 197394\n" +
			"#% 220708\n" +
			"#% 280819\n" +
			"#% 387427\n" +
			"#% 464434\n" +
			"#% 643007\n" +
			"#% 722904\n" +
			"#% 760866\n" +
			"#% 766409\n" +
			"#% 769881\n" +
			"#% 769906\n" +
			"#% 788094\n" +
			"#% 805885\n" +
			"#% 809459\n" +
			"#% 817555\n" +
			"#% 874510\n" +
			"#% 879570\n" +
			"#% 879587\n" +
			"#% 939393\n" +
			"#% 956501\n" +
			"#% 989621\n" +
			"#% 1117023\n" +
			"#% 1250184\n" +
			"#! This paper addresses several key issues \n"+
			//---------------------------------------------
			"#index 2222222\n" +
			"#* ArnetMiner: extraction and mining of academic social networks\n" +
			"#@ Jie Tang;Jing Zhang;Limin Yao;Juanzi Li;Li Zhang;Zhong Su\n" + //6: 2-7
			"#o Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;IBM, Beijing, China;IBM, Beijing, China\n" +
			"#t 2008\n" +
			"#c Proceedings of the 14th ACM SIGKDD international conference on Knowledge discovery and data mining\n" +
			"#% 1000\n" +
			"#% 2000\n" +
			"#% 3000\n" +
			"#% 4000\n" +
			"#% 5000\n" +
			"#% 6000\n" +
			"#% 7000\n" +
			"#% 8000\n" +
			"#% 9000\n" +
			"#% 769881\n" +
			"#% 769906\n" +
			"#% 788094\n" +
			"#% 805885\n" +
			"#% 809459\n" +
			"#% 817555\n" +
			"#% 874510\n" +
			"#% 879570\n" +
			"#% 879587\n" +
			"#% 939393\n" +
			"#% 956501\n" +
			//"#% 989621\n" +
			//"#% 1117023\n" +
			//"#% 1250184\n" +
			"#! This paper addresses several key issues ";
		schema = new Types.ValueType[40];
		schema[0] = Types.ValueType.INT64;
		schema[1] = Types.ValueType.STRING;
		schema[2] = Types.ValueType.STRING;
		schema[3] = Types.ValueType.STRING;
		schema[4] = Types.ValueType.STRING;
		schema[5] = Types.ValueType.STRING;
		schema[6] = Types.ValueType.STRING;
		schema[7] = Types.ValueType.STRING;
		schema[8] = Types.ValueType.STRING;
		schema[9] = Types.ValueType.STRING;
		schema[10] = Types.ValueType.STRING;
		schema[11] = Types.ValueType.STRING;
		schema[12] = Types.ValueType.STRING;
		schema[13] = Types.ValueType.STRING;
		schema[14] = Types.ValueType.INT32;
		schema[15] = Types.ValueType.STRING;
		schema[16] = Types.ValueType.INT64;
		schema[17] = Types.ValueType.INT64;
		schema[18] = Types.ValueType.INT64;
		schema[19] = Types.ValueType.INT64;
		schema[20] = Types.ValueType.INT64;
		schema[21] = Types.ValueType.INT64;
		schema[22] = Types.ValueType.INT64;
		schema[23] = Types.ValueType.INT64;
		schema[24] = Types.ValueType.INT64;
		schema[25] = Types.ValueType.INT64;
		schema[26] = Types.ValueType.INT64;
		schema[27] = Types.ValueType.INT64;
		schema[28] = Types.ValueType.INT64;
		schema[29] = Types.ValueType.INT64;
		schema[30] = Types.ValueType.INT64;
		schema[31] = Types.ValueType.INT64;
		schema[32] = Types.ValueType.INT64;
		schema[33] = Types.ValueType.INT64;
		schema[34] = Types.ValueType.INT64;
		schema[35] = Types.ValueType.INT64;
		schema[36] = Types.ValueType.INT64;
		schema[37] = Types.ValueType.INT64;
		schema[38] = Types.ValueType.INT64;
		schema[39] = Types.ValueType.STRING;

		data = new String[2][40];
		data[0][0] = "1083734";
		data[0][1] = "ArnetMiner: extraction and mining of academic social networks";
		data[0][2] = "Jie Tang";
		data[0][3] = "Jing Zhang";
		data[0][4] = "Limin Yao";
		data[0][5] = "Juanzi Li";
		data[0][6] = "Li Zhang";
		data[0][7] = "Zhong Su";
		data[0][8] = "Tsinghua University, Beijing, China";
		data[0][9] = "Tsinghua University, Beijing, China";
		data[0][10] = "Tsinghua University, Beijing, China";
		data[0][11] = "Tsinghua University, Beijing, China";
		data[0][12] = "IBM, Beijing, China";
		data[0][13] = "IBM, Beijing, China";
		data[0][14] = "2008";
		data[0][15] = "Proceedings of the 14th ACM SIGKDD international conference on Knowledge discovery and data mining";
		data[0][16] = "197394";
		data[0][17] = "220708";
		data[0][18] = "280819";
		data[0][19] = "387427";
		data[0][20] = "464434";
		data[0][21] = "643007";
		data[0][22] = "722904";
		data[0][23] = "760866";
		data[0][24] = "766409";
		data[0][25] = "769881";
		data[0][26] = "769906";
		data[0][27] = "788094";
		data[0][28] = "805885";
		data[0][29] = "809459";
		data[0][30] = "817555";
		data[0][31] = "874510";
		data[0][32] = "879570";
		data[0][33] = "879587";
		data[0][34] = "939393";
		data[0][35] = "956501";
		data[0][36] = "989621";
		data[0][37] = "1117023";
		data[0][38] = "1250184";
		data[0][39] = "This paper addresses several key issues ";

		data[1][0] = "2222222";
		data[1][1] = "ArnetMiner: extraction and mining of academic social networks";
		data[1][2] = "Jie Tang";
		data[1][3] = "Jing Zhang";
		data[1][4] = "Limin Yao";
		data[1][5] = "Juanzi Li";
		data[1][6] = "Li Zhang";
		data[1][7] = "Zhong Su";
		data[1][8] = "Tsinghua University, Beijing, China";
		data[1][9] = "Tsinghua University, Beijing, China";
		data[1][10] = "Tsinghua University, Beijing, China";
		data[1][11] = "Tsinghua University, Beijing, China";
		data[1][12] = "IBM, Beijing, China";
		data[1][13] = "IBM, Beijing, China";
		data[1][14] = "2008";
		data[1][15] = "Proceedings of the 14th ACM SIGKDD international conference on Knowledge discovery and data mining";
		data[1][16] = "1000";
		data[1][17] = "2000";
		data[1][18] = "3000";
		data[1][19] = "4000";
		data[1][20] = "5000";
		data[1][21] = "6000";
		data[1][22] = "7000";
		data[1][23] = "8000";
		data[1][24] = "9000";
		data[1][25] = "769881";
		data[1][26] = "769906";
		data[1][27] = "788094";
		data[1][28] = "805885";
		data[1][29] = "809459";
		data[1][30] = "817555";
		data[1][31] = "874510";
		data[1][32] = "879570";
		data[1][33] = "879587";
		data[1][34] = "939393";
		data[1][35] = "956501";
		data[1][36] = null; //"989621";
		data[1][37] = null;//"1117023";
		data[1][38] = null;//"1250184";
		data[1][39] = "This paper addresses several key issues ";

//		for(int i=0; i<data[0].length; i++)
//			data[1][i] = data[0][i];

		names = new String[data[0].length];
		for(int i = 0; i < names.length; i++)
			names[i] = "C_" + i;

		String[] naStrings = {"NULL", "inf", "NaN"};
		runGenerateReaderTest();
	}

	@Test
	public void test3() {
		sampleRaw = "#index 1488277\n" +
					"#n 11;12;13;14;15;16;17\n" +
					"#a 21;22;23;24;25;26;27\n" +
					"#pc 70\n" +
					"#cn 370\n" +
					"#hi 9\n" +
					"#pi 76.3254\n" +
					"#upi 73.7573\n" +
					"#t 31;32;33;34;35;36;37"+
			"#index 1488277\n" +
			"#n 11;12;13;14;15;16;17\n" +
			"#a 21;22;23;24;25;26;27\n" +
			"#pc 70\n" +
			"#cn 370\n" +
			"#hi 9\n" +
			"#pi 76.3254\n" +
			"#upi 73.7573\n" +
			"#t 31;32;33;34;35;36;37";
		schema = new Types.ValueType[27];
		schema[0] = Types.ValueType.INT64;

		schema[1] = Types.ValueType.STRING; // list
		schema[2] = Types.ValueType.STRING; // list
		schema[3] = Types.ValueType.STRING; // list
		schema[4] = Types.ValueType.STRING; // list
		schema[5] = Types.ValueType.STRING; // list
		schema[6] = Types.ValueType.STRING; // list
		schema[7] = Types.ValueType.STRING; // list

		schema[8] = Types.ValueType.STRING; // list
		schema[9] = Types.ValueType.STRING; // list
		schema[10] = Types.ValueType.STRING; // list
		schema[11] = Types.ValueType.STRING; // list
		schema[12] = Types.ValueType.STRING; // list
		schema[13] = Types.ValueType.STRING; // list
		schema[14] = Types.ValueType.STRING; // list

		schema[15] = Types.ValueType.INT32;
		schema[16] = Types.ValueType.INT32;
		schema[17] = Types.ValueType.INT32;
		schema[18] = Types.ValueType.FP32;
		schema[19] = Types.ValueType.FP32;

		schema[20] = Types.ValueType.STRING; // list
		schema[21] = Types.ValueType.STRING; // list
		schema[22] = Types.ValueType.STRING; // list
		schema[23] = Types.ValueType.STRING; // list
		schema[24] = Types.ValueType.STRING; // list
		schema[25] = Types.ValueType.STRING; // list
		schema[26] = Types.ValueType.STRING; // list


		data = new String[2][27];
		data[0][0] = "1488277";

		data[0][1] = "11";
		data[0][2] = "12";
		data[0][3] = "13";
		data[0][4] = "14";
		data[0][5] = "15";
		data[0][6] = "16";
		data[0][7] = "17";

		data[0][8] = "21";
		data[0][9] = "22";
		data[0][10] = "23";
		data[0][11] = "24";
		data[0][12] = "25";
		data[0][13] = "26";
		data[0][14] = "27";

		data[0][15] = "70";
		data[0][16] = "370";
		data[0][17] = "9";
		data[0][18] = "76.3254";
		data[0][19] = "73.7573";

		data[0][20] = "31";
		data[0][21] = "32";
		data[0][22] = "33";
		data[0][23] = "34";
		data[0][24] = "35";
		data[0][25] = "36";
		data[0][26] = "37";

		for(int i = 0; i < data[0].length; i++)
			data[1][i] = data[0][i];

		names = new String[data[0].length];
		for(int i = 0; i < names.length; i++)
			names[i] = "C_" + i;

		String[] naStrings = {"NULL", "inf", "NaN"};
		runGenerateReaderTest();
	}

	//#index ---- index id of this author
	//#n ---- name  (separated by semicolons)
	//#a ---- affiliations  (separated by semicolons)
	//#pc ---- the count of published papers of this author
	//#cn ---- the total number of citations of this author
	//#hi ---- the H-index of this author
	//#pi ---- the P-index with equal A-index of this author
	//#upi ---- the P-index with unequal A-index of this author
	//#t ---- research interests of this author  (separated by semicolons)


}

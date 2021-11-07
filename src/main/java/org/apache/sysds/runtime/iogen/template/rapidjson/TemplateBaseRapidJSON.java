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

package org.apache.sysds.runtime.iogen.template.rapidjson;

import org.apache.sysds.runtime.iogen.CustomProperties;
import org.apache.sysds.runtime.iogen.template.TemplateBase;

public abstract class TemplateBaseRapidJSON extends TemplateBase {

	protected String headerTemplate;
	protected String sourceTemplate;
	protected String code = "%code%";

	public abstract String getFrameReaderCode(String sourceFileName, String headerFileName);

	public TemplateBaseRapidJSON(CustomProperties _props) {
		super(_props);
		headerTemplate = "#ifndef RAPIDJSONCPP_FRAMEREADERGIO_H \n"+
						 "#define RAPIDJSONCPP_FRAMEREADERGIO_H \n"+
						 "#include \"FrameReader.h\" \n"+
						 "#include <set> \n" +
						 "class FrameReaderGIO : public FrameReader  { \n"+
						 "private: \n"+
						 	code + "\n"+
						 "public: \n"+
						 "FrameReaderGIO(); \n"+
						 "virtual ~FrameReaderGIO(); \n"+
						 "void getJSONValues(vector<long> *col, vector<ItemObject*> *colValue, Document &d) override; \n"+
						 "}; \n"+
						 "#endif //RAPIDJSONCPP_FRAMEREADERGIO_H \n";
		sourceTemplate = "#include \"FrameReaderGIO.h\" \n"+
						 "FrameReaderGIO::FrameReaderGIO() {} \n"+
						 "FrameReaderGIO::~FrameReaderGIO() {} \n"+
						 "void FrameReaderGIO::getJSONValues(vector<long> *col, vector<ItemObject*> *colValue, Document &d) { \n"+
						 " int index = 0; \n"+
						 "int listSize = 0; \n"+
							code+"\n"+
						 "} \n";
	}
}

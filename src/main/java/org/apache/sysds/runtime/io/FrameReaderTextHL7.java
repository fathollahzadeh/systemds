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

package org.apache.sysds.runtime.io;

import ca.uhn.hl7v2.model.Composite;
import ca.uhn.hl7v2.model.DataTypeException;
import ca.uhn.hl7v2.model.Group;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.Primitive;
import ca.uhn.hl7v2.model.Segment;
import ca.uhn.hl7v2.model.Structure;
import ca.uhn.hl7v2.model.Type;
import ca.uhn.hl7v2.model.Varies;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.parser.EncodingCharacters;
import ca.uhn.hl7v2.parser.PipeParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.util.UtilFunctions;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class FrameReaderTextHL7 extends FrameReader {
	protected static FileFormatPropertiesHL7 _props;

	public FrameReaderTextHL7(FileFormatPropertiesHL7 props) {
		//if unspecified use default properties for robustness
		_props = props;
	}

	@Override
	public FrameBlock readFrameFromHDFS(String fname, ValueType[] schema, String[] names, long rlen, long clen)
		throws IOException, DMLRuntimeException {
		LOG.debug("readFrameFromHDFS HL7");
		// prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(fname);
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
		FileInputFormat.addInputPath(job, path);

		// check existence and non-empty file
		checkValidInputFile(fs, path);

		TextInputFormat informat = new TextInputFormat();
		informat.configure(job);
		InputSplit[] splits = informat.getSplits(job, 1);
		splits = IOUtilFunctions.sortInputSplits(splits);

		rlen = computeHL7NRows(informat, job, splits);
		String[] lnames = createOutputNames(names, clen);
		FrameBlock ret = createOutputFrameBlock(schema, lnames, rlen);

		// core read (sequential/parallel)
		readHL7FrameFromHDFS(informat, job, splits, ret, schema);
		return ret;
	}

	protected int computeHL7NRows(TextInputFormat informat, JobConf job, InputSplit[] splits) throws IOException {
		int row = 0;
		LongWritable key = new LongWritable();
		Text value = new Text();
		for(int i = 0; i < splits.length; i++) {
			RecordReader<LongWritable, Text> reader = informat.getRecordReader(splits[i], job, Reporter.NULL);
			while(reader.next(key, value)) {
				String raw = value.toString().trim();
				if(raw.startsWith("MSH|")) {
					row++;
				}
			}
		}
		return row;
	}

	@Override
	public FrameBlock readFrameFromInputStream(InputStream is, ValueType[] schema, String[] names, long rlen, long clen)
		throws IOException, DMLRuntimeException {

		//		// TODO: fix stream reader. incomplete
		//		LOG.debug("readFrameFromInputStream csv");
		//		ValueType[] lschema = null;
		//		String[] lnames = null;
		//
		//		InputStreamInputFormat informat = new InputStreamInputFormat(is);
		//		InputSplit[] splits = informat.getSplits(null, 1);
		//		splits = IOUtilFunctions.sortInputSplits(splits);
		//
		//		if(_props.getType().equals("paper")) {
		//			paperMetaData = computeAMinerSizePaper(null,null, splits);
		//			rlen = paperMetaData.nrow;
		//			lschema = paperMetaData.schema;
		//			lnames = paperMetaData.names;
		//		}
		//		else {
		//			authorMetaData = computeAMinerSizeAuthor(null,null, splits);
		//			rlen = authorMetaData.nrow;
		//			lschema = authorMetaData.schema;
		//			lnames = authorMetaData.names;
		//		}
		//		FrameBlock ret = createOutputFrameBlock(lschema, lnames, rlen);
		//
		//		// core read (sequential/parallel)
		//		if(_props.getType().equals("paper")) {
		//			readAMinerPaperFrameFromInputSplit(splits[0], rowIndexs[0], colBeginIndexs[0], informat, null, ret, schema);
		//		}
		//		else {
		//			readAMinerAuthorFrameFromInputSplit(splits[0], rowIndexs[0], informat, null, ret, schema);
		//		}
		//		return ret;

		return null;

	}

	protected void readHL7FrameFromHDFS(TextInputFormat informat, JobConf job, InputSplit[] splits, FrameBlock dest,
		ValueType[] schema) throws IOException {
		LOG.debug("readHL7FrameFromHDFS Message");
		int rowIndex = 0;
		for(int i = 0; i < splits.length; i++)
			rowIndex = readHL7FrameFromInputSplit(splits[i], rowIndex, informat, job, dest, schema);

	}

	protected final int readHL7FrameFromInputSplit(InputSplit split, int rowIndex,
		InputFormat<LongWritable, Text> informat, JobConf job, FrameBlock dest, ValueType[] schema) throws IOException {

		// create record reader
		RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, job, Reporter.NULL);
		LongWritable key = new LongWritable();
		Text value = new Text();
		int row = rowIndex, col = 0;
		// Read the data
		StringBuilder messageString = new StringBuilder();
		PipeParser pipeParser = new PipeParser();
		try {
			while(reader.next(key, value)) // foreach line
			{
				String rowStr = value.toString().trim();
				if(rowStr.length() == 0)
					continue;

				if(rowStr.startsWith("MSH|")) {
					if(messageString.length() > 0) {
						try {
							// parse HL7 message
							Message message = pipeParser.parse(messageString.toString());
							ArrayList<String> values = new ArrayList<>();
							groupEncode(message, values);
							if(_props.isReadAllValues()) {
								col = 0;
								for(String s : values)
									dest.set(row, col++, UtilFunctions.stringToObject(ValueType.STRING, s));
							}
							else if(_props.isRangeBaseRead()){
								for(int i = 0; i<_props.getMaxColumnIndex();i++)
									dest.set(row, i, UtilFunctions.stringToObject(ValueType.STRING, values.get(i)));
							}
							else{
								for(int i =0; i< _props.getSelectedIndexes().length; i++) {
									dest.set(row, i, UtilFunctions.stringToObject(ValueType.STRING, values.get(_props.getSelectedIndexes()[i])));
								}
							}
						}
						catch(Exception exception) {
							throw new IOException("Can't part hel7 message:", exception);
						}
						row++;
						messageString = new StringBuilder();
					}
				}
				messageString.append(rowStr);
			}
		}
		finally {
			IOUtilFunctions.closeSilently(reader);
		}
		return row;
	}

	protected static void groupEncode(Group groupObject, ArrayList<String> values) {
		String[] childNames = groupObject.getNames();
		try {
			String[] var5 = childNames;
			int var6 = childNames.length;

			for(int var7 = 0; var7 < var6; ++var7) {
				String name = var5[var7];
				Structure[] reps = groupObject.getAll(name);
				Structure[] var10 = reps;
				int var11 = reps.length;

				for(int var12 = 0; var12 < var11; ++var12) {
					Structure rep = var10[var12];
					if(rep instanceof Group) {
						groupEncode((Group) rep, values);
					}
					else if(rep instanceof Segment) {
						segmentEncode((Segment) rep, values);
					}
				}
			}
		}
		catch(Exception exception) {
			exception.printStackTrace();
		}
	}

	protected static void segmentEncode(Segment segmentObject, ArrayList<String> values) throws HL7Exception {
		int n = segmentObject.numFields();
		for(int i = 1; i <= n; ++i) {
			Type[] reps = segmentObject.getField(i);
			Type[] var8 = reps;
			int var9 = reps.length;
			for(int var10 = 0; var10 < var9; ++var10) {
				Type rep = var8[var10];
				encode(rep, values);
			}
		}
	}

	protected static void encode(Type datatypeObject, ArrayList<String> values) throws DataTypeException {
		if(_props.isRangeBaseRead() && values.size()>_props.getMaxColumnIndex())
			return;
		else if(_props.isQueryFilter() && values.size() > _props.getMaxColumnIndex())
			return;
		else {
			if(datatypeObject instanceof Varies) {
				encodeVaries((Varies) datatypeObject, values);
			}
			else if(datatypeObject instanceof Primitive) {
				encodePrimitive((Primitive) datatypeObject, values);
			}
			else if(datatypeObject instanceof Composite) {
				encodeComposite((Composite) datatypeObject, values);
			}
		}
	}

	protected static void encodeVaries(Varies datatypeObject, ArrayList<String> values) throws DataTypeException {
		if(datatypeObject.getData() != null) {
			encode(datatypeObject.getData(), values);
		}
	}

	protected static void encodePrimitive(Primitive datatypeObject, ArrayList<String> values) throws DataTypeException {
		String value = datatypeObject.getValue();
		boolean hasValue = value != null && value.length() > 0;
		if(hasValue) {
			try {
				EncodingCharacters ec = EncodingCharacters.getInstance(datatypeObject.getMessage());
				char esc = ec.getEscapeCharacter();
				int oldpos = 0;

				int pos;
				boolean escaping;
				for(escaping = false; (pos = value.indexOf(esc, oldpos)) >= 0; oldpos = pos + 1) {
					String v = value.substring(oldpos, pos);
					if(!escaping) {
						escaping = true;
					}
					else if(!v.startsWith(".") && !"H".equals(v) && !"N".equals(v)) {
					}
					else {
						escaping = false;
					}
				}

				if(oldpos <= value.length()) {
					StringBuilder sb = new StringBuilder();
					if(escaping) {
						sb.append(esc);
					}
					sb.append(value.substring(oldpos));
					values.add(sb.toString());
				}
			}
			catch(Exception var12) {
				throw new DataTypeException("Exception encoding Primitive: ", var12);
			}
		}
	}

	protected static void encodeComposite(Composite datatypeObject, ArrayList<String> values) throws DataTypeException {
		Type[] components = datatypeObject.getComponents();
		for(int i = 0; i < components.length; ++i) {
			encode(components[i], values);
		}
	}
}
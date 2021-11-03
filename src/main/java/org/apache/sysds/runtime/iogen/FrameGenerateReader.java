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

package org.apache.sysds.runtime.iogen;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.sysds.common.Types;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.io.FrameReader;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.Pair;
import org.apache.sysds.runtime.util.InputStreamInputFormat;
import org.apache.sysds.runtime.util.UtilFunctions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class FrameGenerateReader extends FrameReader {

	protected CustomProperties _props;
	protected final FastStringTokenizer fastStringTokenizerDelim;

	public FrameGenerateReader(CustomProperties _props) {
		this._props = _props;
		fastStringTokenizerDelim = new FastStringTokenizer(_props.getDelim());
	}

	private int getNumRows(List<Path> files, FileSystem fs) throws IOException, DMLRuntimeException {
		int rows = 0;
		String value;
		for(int fileNo = 0; fileNo < files.size(); fileNo++) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(files.get(fileNo))));
			try {
				// Row Regular
				if(_props.getRowPattern().equals(CustomProperties.GRPattern.Regular)) {
					// TODO: check the file has header?
					while(br.readLine() != null)
						rows++;
				}
				// Row Irregular
				else {
					FastStringTokenizer st = new FastStringTokenizer(_props.getDelim());
					while((value = br.readLine()) != null) {
						st.reset(value);
						int row = st.nextInt();
						rows = Math.max(rows, row);
					}
					rows++;
				}
			}
			finally {
				IOUtilFunctions.closeSilently(br);
			}
		}
		return rows;
	}

	@Override public FrameBlock readFrameFromHDFS(String fname, Types.ValueType[] schema, String[] names, long rlen,
		long clen) throws IOException, DMLRuntimeException {

		// prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(fname);
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
		FileInputFormat.addInputPath(job, path);

		// check existence and non-empty file
		checkValidInputFile(fs, path);

		// compute size if necessary
		if(rlen <= 0) {
			ArrayList<Path> paths = new ArrayList<>();
			paths.add(path);
			rlen = getNumRows(paths, fs);
		}

		// allocate output frame block
		Types.ValueType[] lschema = createOutputSchema(schema, clen);
		String[] lnames = createOutputNames(names, clen);
		FrameBlock ret = createOutputFrameBlock(lschema, lnames, rlen);

		// core read (sequential/parallel)
		readFrameFromHDFS(path, job, fs, ret, lschema, lnames, rlen, clen);

		return ret;

	}

	@Override public FrameBlock readFrameFromInputStream(InputStream is, Types.ValueType[] schema, String[] names,
		long rlen, long clen) throws IOException, DMLRuntimeException {

		// allocate output frame block
		Types.ValueType[] lschema = createOutputSchema(schema, clen);
		String[] lnames = createOutputNames(names, clen);
		FrameBlock ret = createOutputFrameBlock(lschema, lnames, rlen);

		// core read (sequential/parallel)
		InputStreamInputFormat informat = new InputStreamInputFormat(is);
		InputSplit split = informat.getSplits(null, 1)[0];
		readFrameFromInputSplit(split, informat, null, ret, schema, names, rlen, clen, 0, true);

		return ret;
	}

	protected void readFrameFromHDFS(Path path, JobConf job, FileSystem fs, FrameBlock dest, Types.ValueType[] schema,
		String[] names, long rlen, long clen) throws IOException {

		TextInputFormat informat = new TextInputFormat();
		informat.configure(job);
		InputSplit[] splits = informat.getSplits(job, 0);
		splits = IOUtilFunctions.sortInputSplits(splits);
		for(int i = 0, rpos = 0; i < splits.length; i++)
			rpos = readFrameFromInputSplit(splits[i], informat, job, dest, schema, names, rlen, clen, rpos, i == 0);
	}

	protected abstract int readFrameFromInputSplit(InputSplit split, InputFormat<LongWritable, Text> informat,
		JobConf job, FrameBlock dest, Types.ValueType[] schema, String[] names, long rlen, long clen, int rl,
		boolean first) throws IOException;

	public static class FrameReaderRowRegularColRegular extends FrameGenerateReader {

		public FrameReaderRowRegularColRegular(CustomProperties _props) {
			super(_props);
		}

		@Override protected int readFrameFromInputSplit(InputSplit split, InputFormat<LongWritable, Text> informat,
			JobConf job, FrameBlock dest, Types.ValueType[] schema, String[] names, long rlen, long clen, int rl,
			boolean first) throws IOException {

			String cellValue;
			fastStringTokenizerDelim.setNaStrings(_props.getNaStrings());

			// create record reader
			RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, job, Reporter.NULL);
			LongWritable key = new LongWritable();
			Text value = new Text();
			int row = rl;
			int col = 0;
			Set<String> naValues = _props.getNaStrings();

			// Read the data
			try {
				while(reader.next(key, value)) // foreach line
				{
					String cellStr = value.toString();
					fastStringTokenizerDelim.reset(cellStr);
					while(col != -1) {
						cellValue = fastStringTokenizerDelim.nextToken();
						col = fastStringTokenizerDelim.getIndex();
						if(col != -1 && cellValue != null && (naValues == null || !naValues.contains(cellValue))) {
							dest.set(row, col, UtilFunctions.stringToObject(schema[col], cellValue));
						}
					}
					row++;
					col = 0;
				}
			}
			finally {
				IOUtilFunctions.closeSilently(reader);
			}
			return row;
		}
	}

	public static class FrameReaderRowRegularColIrregular extends FrameGenerateReader {

		public FrameReaderRowRegularColIrregular(CustomProperties _props) {
			super(_props);
		}

		@Override protected int readFrameFromInputSplit(InputSplit split, InputFormat<LongWritable, Text> informat,
			JobConf job, FrameBlock dest, Types.ValueType[] schema, String[] names, long rlen, long clen, int rl,
			boolean first) throws IOException {

			String cellValue;
			FastStringTokenizer fastStringTokenizerIndexDelim = new FastStringTokenizer(_props.getIndexDelim());

			// create record reader
			RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, job, Reporter.NULL);
			LongWritable key = new LongWritable();
			Text value = new Text();
			int row = rl;
			int col = 0;

			// Read the data
			try {
				while(reader.next(key, value)) // foreach line
				{
					String cellStr = value.toString();
					fastStringTokenizerDelim.reset(cellStr);
					String cellValueString = fastStringTokenizerDelim.nextToken();
					dest.set(row, (int) clen - 1 - _props.getFirstColIndex(),
						UtilFunctions.stringToObject(schema[(int) clen - 1 - _props.getFirstColIndex()],
							cellValueString));

					while(col != -1) {
						String nt = fastStringTokenizerDelim.nextToken();
						if(fastStringTokenizerDelim.getIndex() == -1)
							break;
						fastStringTokenizerIndexDelim.reset(nt);
						col = fastStringTokenizerIndexDelim.nextInt();
						cellValue = fastStringTokenizerIndexDelim.nextToken();
						if(col != -1 && cellValue != null) {
							dest.set(row, col - _props.getFirstColIndex(),
								UtilFunctions.stringToObject(schema[col - _props.getFirstColIndex()], cellValue));
						}
					}
					row++;
					col = 0;
				}
			}
			finally {
				IOUtilFunctions.closeSilently(reader);
			}
			return row;
		}
	}

	public static class FrameReaderRowIrregularColRegular extends FrameGenerateReader {

		public FrameReaderRowIrregularColRegular(CustomProperties _props) {
			super(_props);
		}

		@Override protected int readFrameFromInputSplit(InputSplit split, InputFormat<LongWritable, Text> informat,
			JobConf job, FrameBlock dest, Types.ValueType[] schema, String[] names, long rlen, long clen, int rl,
			boolean first) throws IOException {

			String cellValue;
			fastStringTokenizerDelim.setNaStrings(_props.getNaStrings());

			// create record reader
			RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, job, Reporter.NULL);
			LongWritable key = new LongWritable();
			Text value = new Text();
			int row = rl;
			int col = 0;

			// Read the data
			try {
				while(reader.next(key, value)) // foreach line
				{
					String cellStr = value.toString();
					fastStringTokenizerDelim.reset(cellStr);
					int ri = fastStringTokenizerDelim.nextInt();
					col = fastStringTokenizerDelim.nextInt();
					cellValue = fastStringTokenizerDelim.nextToken();

					if(col != -1 && cellValue != null) {
						dest.set(ri - _props.getFirstRowIndex(), col - _props.getFirstColIndex(),
							UtilFunctions.stringToObject(schema[col - _props.getFirstColIndex()], cellValue));
					}
					row = Math.max(row, ri);
				}
			}
			finally {
				IOUtilFunctions.closeSilently(reader);
			}
			return row;
		}
	}

	public static class FrameReaderRowIrregularColIrregular extends FrameGenerateReader {

		public FrameReaderRowIrregularColIrregular(CustomProperties _props) {
			super(_props);
		}

		@Override protected int readFrameFromInputSplit(InputSplit split, InputFormat<LongWritable, Text> informat,
			JobConf job, FrameBlock dest, Types.ValueType[] schema, String[] names, long rlen, long clen, int rl,
			boolean first) throws IOException {

			// create record reader
			RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, job, Reporter.NULL);
			int row = rl;
			Pair<ArrayList<String>, ArrayList<Integer>> uniquePrefixes = new Pair<>(new ArrayList<>(),
				new ArrayList<>());
			for(String p : _props.getColPrefixes()) {
				int lastIndex = uniquePrefixes.getKey().size() - 1;
				if(lastIndex >= 0 && uniquePrefixes.getKey().get(lastIndex).equals(p)) {
					uniquePrefixes.getValue().set(lastIndex, uniquePrefixes.getValue().get(lastIndex) + 1);
				}
				else {
					uniquePrefixes.getKey().add(p);
					uniquePrefixes.getValue().add(1);
				}
			}
			String[] prefixes = uniquePrefixes.getKey().toArray(new String[0]);
			Integer[] prefixesCount = uniquePrefixes.getValue().toArray(new Integer[0]);

			// Read the data
			int uniquePrefixIndex = 0;
			int col = 0;
			int nextIndex1 = 0;
			String strValue = null;
			int index1, index2, currUniqueIndex;

			try {
				while(true) {
					strValue = strValue==null ? getNewLineOfData(reader) : strValue;
					if(strValue == null)
						break;
					currUniqueIndex = 0;
					int lastCol = col;
					do {
						index1 = strValue.indexOf(prefixes[uniquePrefixIndex]);
						if(index1 ==-1){
							col+= prefixesCount[uniquePrefixIndex];
							uniquePrefixIndex++;
						}
						else {
							currUniqueIndex = uniquePrefixIndex;
							nextIndex1 = index1 + prefixes[currUniqueIndex].length();
						}
					} while(index1==-1 && uniquePrefixIndex < prefixes.length);

					// Skip the row if we don't have any match on delimiters
					if(index1 == -1){
						if(col>0 && lastCol !=0) {
							row++;
						}
						col = 0;
						uniquePrefixIndex = 0;
						strValue = null;
						continue;
					}
					index2 = -1;
					int skipCols = 0;
					uniquePrefixIndex++;

					while(index2 ==-1 && uniquePrefixIndex < prefixes.length) {
						if(prefixes[currUniqueIndex].equals(prefixes[uniquePrefixIndex]))
							break;
						index2 = strValue.indexOf(prefixes[uniquePrefixIndex], nextIndex1);
						if(index2 ==-1){
							skipCols+= prefixesCount[uniquePrefixIndex];
							uniquePrefixIndex++;
						}
					}
					if(index2 == -1) {
						index2 = strValue.length();
						skipCols = 0;
					}
					col+=skipCols;

					StringBuilder str = new StringBuilder(strValue.substring(nextIndex1, index2));
					String newStr="";
					// Get data
					if(prefixesCount[currUniqueIndex] == 1) {
						if(str.length() > 0)
							dest.set(row, col, UtilFunctions.stringToObject(schema[col], str.toString()));
						col++;
					}
					else {
						do {
							newStr = getNewLineOfData(reader);
							if(newStr!=null){
								if(newStr.startsWith(prefixes[currUniqueIndex])) {
									str.append(newStr);
								}
								else
									break;
							}
							else
								break;
						} while(true);

						if(newStr==null)
							newStr="";

						String[] vl = str.toString().split(prefixes[currUniqueIndex], -1);
						int minL = Math.min(vl.length, prefixesCount[currUniqueIndex]);
						skipCols = prefixesCount[currUniqueIndex] - minL;
						for(int i=0; i< minL;i++){
							if(vl[i].length()>0)
								dest.set(row, col, UtilFunctions.stringToObject(schema[col], vl[i]));
							col++;
						}
						col += skipCols;
					}

					uniquePrefixIndex = currUniqueIndex+1;
					if(uniquePrefixIndex >= prefixes.length) {
						uniquePrefixIndex = 0;
						col = 0;
						row++;
					}
					strValue = strValue.substring(index2)+newStr;
					if(strValue.length()==0)
						strValue = null;
				}
			}
			finally {
				IOUtilFunctions.closeSilently(reader);
			}
			return row;
		}

		private static String getNewLineOfData(RecordReader<LongWritable, Text> reader) throws IOException {
			LongWritable key = new LongWritable();
			Text value = new Text();
			if(reader.next(key, value)){
				return value.toString();
			}
			else
				return null;
		}
	}

	public static class FrameReaderJSON extends FrameGenerateReader {
		public FrameReaderJSON(CustomProperties _props) {
			super(_props);
		}

		@Override protected int readFrameFromInputSplit(InputSplit split, InputFormat<LongWritable, Text> informat,
			JobConf job, FrameBlock dest, Types.ValueType[] schema, String[] names, long rlen, long clen, int rl,
			boolean first) throws IOException {

			// create record reader
			RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, job, Reporter.NULL);
			LongWritable key = new LongWritable();
			Text value = new Text();
			int row = rl;
			String[] colKeys = _props.getColKeys();
			Object cellValue;
			// Read the data
			try {
				while(reader.next(key, value)) {
					FastJSONIndex fastJSONIndex = new FastJSONIndex(value.toString());
					for(int c = 0; c < clen; c++) {
						cellValue = fastJSONIndex.getObjectValue(colKeys[c]);
						if(cellValue != null) {
							dest.set(row, c, UtilFunctions.objectToObject(schema[c], cellValue));
						}
					}
					row++;
				}
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
			finally {
				IOUtilFunctions.closeSilently(reader);
			}
			return row;
		}
	}
}

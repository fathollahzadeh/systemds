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

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.iogen.template.TemplateUtil;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.Pair;
import org.apache.sysds.runtime.util.CommonThreadPool;
import org.apache.sysds.runtime.util.UtilFunctions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Multi-threaded frame text HL7 reader.
 */
public class FrameReaderTextHL7Parallel extends FrameReaderTextHL7 {
	protected int _numThreads;
	protected JobConf job;
	protected TemplateUtil.SplitOffsetInfos _offsets;
	protected int _rLen;
	protected int _cLen;

	public FrameReaderTextHL7Parallel(FileFormatPropertiesHL7 props) {
		super(props);
		this._numThreads = OptimizerUtils.getParallelTextReadParallelism();
	}

	@Override
	public FrameBlock readFrameFromHDFS(String fname, ValueType[] schema, String[] names, long rlen, long clen)
		throws IOException, DMLRuntimeException {

		job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(fname);
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);

		FileInputFormat.addInputPath(job, path);
		TextInputFormat informat = new TextInputFormat();
		informat.configure(job);

		InputSplit[] splits = informat.getSplits(job, _numThreads);
		splits = IOUtilFunctions.sortInputSplits(splits);

		// check existence and non-empty file
		checkValidInputFile(fs, path);

		FrameBlock ret = computeSizeAndCreateOutputFrameBlock(informat, job, schema, names, splits, "MSH|");

		readHL7FrameFromHDFS(splits, informat, job, schema, ret);

		return ret;
	}

	protected void readHL7FrameFromHDFS(InputSplit[] splits, TextInputFormat informat, JobConf jobConf,
		Types.ValueType[] schema, FrameBlock dest) throws IOException {

		ExecutorService pool = CommonThreadPool.get(_numThreads);
		try {
			// create read tasks for all splits
			ArrayList<ReadTask> tasks = new ArrayList<>();
			int splitCount = 0;
			for(InputSplit split : splits) {
				tasks.add(new ReadTask(split, informat, dest, splitCount++, schema));
			}
			pool.invokeAll(tasks);
			pool.shutdown();

		}
		catch(Exception e) {
			throw new IOException("Threadpool issue, while parallel read.", e);
		}
	}

	protected FrameBlock computeSizeAndCreateOutputFrameBlock(TextInputFormat informat, JobConf job,
		Types.ValueType[] schema, String[] names, InputSplit[] splits, String beginToken)
		throws IOException, DMLRuntimeException {
		_rLen = 0;
		_cLen = names.length;

		// count rows in parallel per split
		try {
			ExecutorService pool = CommonThreadPool.get(_numThreads);

			_offsets = new TemplateUtil.SplitOffsetInfos(splits.length);
			for(int i = 0; i < splits.length; i++) {
				TemplateUtil.SplitInfo splitInfo = new TemplateUtil.SplitInfo();
				_offsets.setSeqOffsetPerSplit(i, splitInfo);
				_offsets.setOffsetPerSplit(i, 0);
			}

			ArrayList<CountRowsTask> tasks = new ArrayList<>();
			int splitIndex = 0;
			for(InputSplit split : splits) {
				Integer nextOffset = splitIndex + 1 == splits.length ? null : splitIndex + 1;
				tasks.add(new CountRowsTask(_offsets, splitIndex, nextOffset, split, informat, job, beginToken));
				splitIndex++;
			}

			// collect row counts for offset computation
			int i = 0;
			for(Future<Integer> rc : pool.invokeAll(tasks)) {
				Integer nrows = rc.get();
				_offsets.setOffsetPerSplit(i, _rLen);
				_rLen += nrows;
				i++;
			}
			pool.shutdown();

		}
		catch(Exception e) {
			throw new IOException("Thread pool Error " + e.getMessage(), e);
		}
		FrameBlock ret = createOutputFrameBlock(schema, names, _rLen);
		return ret;
	}

	private static class CountRowsTask implements Callable<Integer> {
		private final TemplateUtil.SplitOffsetInfos _offsets;
		private final Integer _curOffset;
		private final Integer _nextOffset;
		private final InputSplit _split;
		private final TextInputFormat _inputFormat;
		private final JobConf _job;
		private final String _beginToken;

		public CountRowsTask(TemplateUtil.SplitOffsetInfos offsets, Integer curOffset, Integer nextOffset,
			InputSplit split, TextInputFormat inputFormat, JobConf job, String beginToken) {
			_offsets = offsets;
			_curOffset = curOffset;
			_nextOffset = nextOffset;
			_inputFormat = inputFormat;
			_split = split;
			_job = job;
			_beginToken = beginToken;
		}

		@Override
		public Integer call() throws Exception {
			int nrows = 0;

			ArrayList<Pair<Long, Integer>> beginIndexes = TemplateUtil.getTokenIndexOnMultiLineRecords(_split,
				_inputFormat, _job, _beginToken).getKey();
			ArrayList<Pair<Long, Integer>> endIndexes = new ArrayList<>();
			for(int i = 1; i < beginIndexes.size(); i++)
				endIndexes.add(beginIndexes.get(i));
			int tokenLength = _beginToken.length();

			int i = 0;
			int j = 0;

			if(beginIndexes.get(0).getKey() > 0)
				nrows++;

			while(i < beginIndexes.size() && j < endIndexes.size()) {
				Pair<Long, Integer> p1 = beginIndexes.get(i);
				Pair<Long, Integer> p2 = endIndexes.get(j);
				int n = 0;
				while(p1.getKey() < p2.getKey() || (p1.getKey() == p2.getKey() && p1.getValue() < p2.getValue())) {
					n++;
					i++;
					if(i == beginIndexes.size()) {
						break;
					}
					p1 = beginIndexes.get(i);
				}
				j += n - 1;
				_offsets.getSeqOffsetPerSplit(_curOffset)
					.addIndexAndPosition(beginIndexes.get(i - n).getKey(), endIndexes.get(j).getKey(),
						beginIndexes.get(i - n).getValue(), endIndexes.get(j).getValue());
				j++;
				nrows++;
			}
			if(_nextOffset != null) {
				RecordReader<LongWritable, Text> reader = _inputFormat.getRecordReader(_split, _job, Reporter.NULL);
				LongWritable key = new LongWritable();
				Text value = new Text();

				StringBuilder sb = new StringBuilder();

				for(long ri = 0; ri < beginIndexes.get(beginIndexes.size() - 1).getKey(); ri++) {
					reader.next(key, value);
				}
				if(reader.next(key, value)) {
					String strVar = value.toString();
					sb.append(strVar.substring(beginIndexes.get(beginIndexes.size() - 1).getValue()));
					while(reader.next(key, value)) {
						sb.append(value.toString());
					}
					_offsets.getSeqOffsetPerSplit(_nextOffset).setRemainString(sb.toString());
				}
			}
			else {
				nrows++;
				_offsets.getSeqOffsetPerSplit(_curOffset)
					.addIndexAndPosition(endIndexes.get(endIndexes.size() -1).getKey(),	_split.getLength()-1,0, 0);
			}
			_offsets.getSeqOffsetPerSplit(_curOffset).setNrows(nrows);
			_offsets.setOffsetPerSplit(_curOffset, nrows);

			return nrows;
		}
	}

	private class ReadTask implements Callable<Long> {

		private final InputSplit _split;
		private final TextInputFormat _informat;
		private final FrameBlock _dest;
		private final int _splitCount;
		private final Types.ValueType[] _schema;

		public ReadTask(InputSplit split, TextInputFormat informat, FrameBlock dest, int splitCount,
			Types.ValueType[] schema) {
			_split = split;
			_informat = informat;
			_dest = dest;
			_splitCount = splitCount;
			_schema = schema;
		}

		@Override
		public Long call() throws IOException {
			RecordReader<LongWritable, Text> reader = _informat.getRecordReader(_split, job, Reporter.NULL);
			LongWritable key = new LongWritable();
			Text value = new Text();
			int row = _offsets.getOffsetPerSplit(_splitCount);
			TemplateUtil.SplitInfo _splitInfo = _offsets.getSeqOffsetPerSplit(_splitCount);
			readHL7FrameFromInputSplit(reader, _splitInfo, key, value, row, _schema, _dest);
			return 0L;
		}
	}

	private static void addRow(String messageString, PipeParser pipeParser, FrameBlock dest, Types.ValueType[] schema,
		int row) throws IOException {
		if(messageString.length() > 0) {
			try {
				// parse HL7 message
				Message message = pipeParser.parse(messageString.toString());
				ArrayList<String> values = new ArrayList<>();
				groupEncode(message, values);
				if(_props.isReadAllValues()) {
					int col = 0;
					for(String s : values)
						dest.set(row, col++, UtilFunctions.stringToObject(schema[col], s));
				}
				else if(_props.isRangeBaseRead()) {
					for(int i = 0; i < _props.getMaxColumnIndex(); i++)
						dest.set(row, i, UtilFunctions.stringToObject(schema[i], values.get(i)));
				}
				else {
					for(int i = 0; i < _props.getSelectedIndexes().length; i++) {
						dest.set(row, i,
							UtilFunctions.stringToObject(schema[_props.getSelectedIndexes()[i]], values.get(_props.getSelectedIndexes()[i])));
					}
				}
			}
			catch(Exception exception) {
				throw new IOException("Can't part hel7 message:", exception);
			}
		}
	}

	protected static int readHL7FrameFromInputSplit(RecordReader<LongWritable, Text> reader,
		TemplateUtil.SplitInfo splitInfo, LongWritable key, Text value, int rpos, Types.ValueType[] schema,
		FrameBlock dest) throws IOException {
		int rlen = splitInfo.getNrows();
		int ri;
		int row = 0;
		int beginPosStr, endPosStr;
		String remainStr = "";
		String str = "";
		StringBuilder sb = new StringBuilder(splitInfo.getRemainString());
		long beginIndex = splitInfo.getRecordIndexBegin(0);
		long endIndex = splitInfo.getRecordIndexEnd(0);
		boolean flag;

		PipeParser pipeParser = new PipeParser();
		if(sb.length() > 0) {
			ri = 0;
			while(ri < beginIndex) {
				reader.next(key, value);
				sb.append(value.toString());
				ri++;
			}
			reader.next(key, value);
			String valStr = value.toString();
			sb.append(valStr.substring(0, splitInfo.getRecordPositionBegin(0)));

			addRow(sb.toString(), pipeParser, dest, schema, row + rpos);
			row++;
			sb = new StringBuilder(valStr.substring(splitInfo.getRecordPositionBegin(0)));
		}
		else {
			ri = -1;
		}

		int rowCounter = 0;
		while(row < rlen) {
			flag = reader.next(key, value);
			if(flag) {
				ri++;
				String valStr = value.toString();
				if(ri >= beginIndex && ri <= endIndex) {
					beginPosStr = ri == beginIndex ? splitInfo.getRecordPositionBegin(rowCounter) : 0;
					endPosStr = ri == endIndex ? splitInfo.getRecordPositionEnd(rowCounter) : valStr.length();
					sb.append(valStr.substring(beginPosStr, endPosStr));
					remainStr = valStr.substring(endPosStr);
					continue;
				}
				else {
					str = sb.toString();
					sb = new StringBuilder();
					sb.append(remainStr).append(valStr);
					if(rowCounter + 1 < splitInfo.getListSize()) {
						beginIndex = splitInfo.getRecordIndexBegin(rowCounter + 1);
						endIndex = splitInfo.getRecordIndexEnd(rowCounter + 1);
					}
					rowCounter++;
				}
			}
			else {
				str = sb.toString();
				sb = new StringBuilder();
			}
			addRow(str, pipeParser, dest, schema, row);
			row++;
		}
		return row + rpos;
	}
}

package org.apache.sysds.runtime.iogen.template.javajson;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.iogen.CustomProperties;
import org.apache.sysds.runtime.iogen.FastJSONIndex;
import org.apache.sysds.runtime.iogen.FrameGenerateReader;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.util.UtilFunctions;

import java.io.IOException;

public class myFrameReader extends FrameGenerateReader {
	public myFrameReader(CustomProperties _props) {
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
						dest.set(row, c, 10);
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

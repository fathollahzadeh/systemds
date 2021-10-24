package org.apache.sysds.runtime.iogen.exp;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.io.FrameReader;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.apache.sysds.runtime.matrix.data.FrameBlock;

public class GIONestedExperiment {

	public static void main(String[] args) throws Exception {

		String sampleRawFileName = args[0];
		String sampleFrameFileName = args[1];
		Integer sampleNRows = Integer.parseInt(args[2]);
		String delimiter = args[3];
		String schemaFileName = args[4];
		String dataFileName = args[5];

		Float percent = Float.parseFloat(args[6]);
		String datasetName = args[7];
		String LOG_HOME =args[8];

		if(delimiter.equals("\\t"))
			delimiter = "\t";

		System.out.println("sampleRawFileName="+sampleRawFileName);
		Util util = new Util();
		Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
		int ncols = sampleSchema.length;

		FrameBlock sampleFrame = new FrameBlock(sampleSchema, util.loadFrameData(sampleFrameFileName, sampleNRows, ncols, delimiter));

		double tmpTime = System.nanoTime();
		String sampleRaw = util.readEntireTextFile(sampleRawFileName);
		GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);
		FrameReader fr = gr.getReader();
		double generateTime = (System.nanoTime() - tmpTime) / 1000000000.0;

		tmpTime = System.nanoTime();
		FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, gr.getProperties().getSchema(), -1, gr.getProperties().getSchema().length);
		double readTime = (System.nanoTime() - tmpTime) / 1000000000.0;

		//dataset,data_nrows,data_ncols,col_index_percent,generate_time,read_time
		String log= datasetName+","+ frameBlock.getNumRows()+","+ ncols+","+percent+","+generateTime+","+readTime;
		util.addLog(LOG_HOME, log);
	}
}

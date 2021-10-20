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

		if(delimiter.equals("\\t"))
			delimiter = "\t";

		Util util = new Util();
		Types.ValueType[] sampleSchema = util.getSchema(schemaFileName);
		int ncols = sampleSchema.length;

		FrameBlock sampleFrame = new FrameBlock(sampleSchema,
			util.loadFrameData(sampleFrameFileName, sampleNRows, ncols, delimiter));

		String sampleRaw = util.readEntireTextFile(sampleRawFileName);
		GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);
		FrameReader fr = gr.getReader();
		FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, sampleSchema, -1, ncols);
	}
}

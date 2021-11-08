package org.apache.sysds.runtime.iogen.exp;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.io.FrameReaderTextCSV;
import org.apache.sysds.runtime.matrix.data.FrameBlock;

public class SYSDSFrameExperimentHDFS {

	public static void main(String[] args) throws Exception {

		String delimiter = " ";//args[0];
		String schemaFileName = args[1];
		String dataFileName = args[2];
		String datasetName = args[3];
		String LOG_HOME =args[4];

		if(delimiter.equals("\\t"))
			delimiter = "\t";

		Util util = new Util();
		Types.ValueType[] schema = util.getSchema(schemaFileName);
		int ncols = schema.length;

		double tmpTime = System.nanoTime();
		FileFormatPropertiesCSV csvpro = new FileFormatPropertiesCSV(false, delimiter, false);
		FrameReaderTextCSV csv = new FrameReaderTextCSV(csvpro);
		FrameBlock frameBlock = csv.readFrameFromHDFS(dataFileName, schema,-1,ncols);

		double readTime = (System.nanoTime() - tmpTime) / 1000000000.0;

		String log= datasetName+","+ frameBlock.getNumRows()+","+ ncols+",1.0,0,0,"+readTime;
		util.addLog(LOG_HOME, log);
	}
}

package org.apache.sysds.runtime.iogen.exp;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.io.*;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.apache.sysds.runtime.matrix.data.FrameBlock;

public class frame {

	public static void main(String[] args) throws Exception {

		String[] datasets = {"CSV"};//, "LIBSVM-FZ", "LIBSVM-FO", "MM-FZ", "MM-FO"};

		String DATA_HOME = "/media/sfathollahzadeh/Windows/saeedData/Dataset/Dataset4";
		String LOG_HOME ="/media/sfathollahzadeh/Windows/saeedData/Dataset/Dataset4/LOG/";

		Util u = new Util();
		int runs = 2;
		int nrows = 50000;
		int ncols = 1000;
		float sparsity = 1.0f;
		String fileName = "_nrows_" + nrows + "_ncols_" + ncols + "_sparsity_" + sparsity;

		double mapping_time;
		double analysis_time;
		double read_time;
		long ram_nlines;
		int e=10;


		ProcessBuilder processBuilder = new ProcessBuilder();

		Types.ValueType[] schema =u.getSchema(DATA_HOME+"/data/data"+fileName+".schema");
		String head = "dataset,sparsity,data_nrows,data_ncols,matrix_size,mapping_time,analysis_time,read_time";
		for(String d : datasets) {
			boolean flagFile = false;
			String logFileName = LOG_HOME+"/synthetic-"+d+".csv";

			for(int i = 100; i <=300; i+=100) {

				mapping_time = 0;
				analysis_time = 0;
				read_time = 0;

				for(int r = 0; r < runs; r++) {
					processBuilder.command("/home/sfathollahzadeh/Documents/GitHub/systemds/src/test/java/org/apache/sysds/test/functions/iogen/experiment/runGIONestedExp.sh");
					Process process = processBuilder.start();
					String dataFileName = DATA_HOME + "/data/" + d + fileName+".raw";
					int clen = ncols;
					String dn = d;
					int col = i;
					int row = i;
					if(d.contains("LIBSVM")) {
						clen++;
						dn = "LIBSVM";
						col++;
						row = 2 * i;
					}
					else if(d.contains("MM-FZ") || d.contains("MM-FO")) {
						dn = "MM";
					}
					String rawFileName = DATA_HOME + "/samples/" + d + "_nrows_" + i + "_ncols_" + i + "_sparsity_" + sparsity + ".raw";
					String sampleFrameFileName = DATA_HOME + "/samples/" + dn + "_nrows_" + i + "_ncols_" + i + "_sparsity_" + sparsity+ ".frame";
					String sampleSchemaFileName = DATA_HOME + "/samples/" + dn + "_nrows_" + i + "_ncols_" + i + "_sparsity_" + sparsity + ".schema";

					Types.ValueType[] sampleSchema = u.getSchema(sampleSchemaFileName);


					FileFormatPropertiesCSV csvpro = new FileFormatPropertiesCSV(false, ",", false);
					FrameReaderTextCSV csv = new FrameReaderTextCSV(csvpro);
					FrameBlock sampleFrame = csv.readFrameFromHDFS(sampleFrameFileName, sampleSchema,row,col);

					String sampleRaw = u.readEntireTextFile(rawFileName);

					long tmpTime = System.nanoTime();

					GenerateReader.GenerateReaderFrame gr = new GenerateReader.GenerateReaderFrame(sampleRaw, sampleFrame);
					mapping_time += (System.nanoTime() - tmpTime) / 1000000000.0;

					tmpTime = System.nanoTime();
					FrameReader fr = gr.getReader();
					analysis_time += (System.nanoTime() - tmpTime) / 1000000000.0;

					tmpTime = System.nanoTime();
					FrameBlock frameBlock = fr.readFrameFromHDFS(dataFileName, schema,-1,ncols);
					read_time += (System.nanoTime() - tmpTime) / 1000000000.0;
				}
				// LOG:
				if(!flagFile){
					u.createLog(logFileName,head);
					flagFile = true;
				}
				String log=d+","+sparsity+","+ nrows+","+ ncols+",("+i+"×"+i+"),"+mapping_time/runs+","+analysis_time/runs+","+read_time/runs;
				u.addLog(logFileName, log);
				System.out.println(log);
			}
		}
	}
}

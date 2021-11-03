package org.apache.sysds.runtime.iogen.exp;

import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.io.MatrixReader;
import org.apache.sysds.runtime.io.ReaderTextCSV;
import org.apache.sysds.runtime.iogen.GenerateReader;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

public class matrix {

	public static void main(String[] args) throws Exception {

		String[] datasets = {"CSV", "LIBSVM-FZ", "LIBSVM-FO", "MM-FZ", "MM-FO", "MM-FZ-SYM-UT", "MM-FZ-SYM-LT",
			"MM-FO-SYM-UT", "MM-FO-SYM-LT", "MM-FZ-SKEW-UT", "MM-FZ-SKEW-LT", "MM-FO-SKEW-UT", "MM-FO-SKEW-LT"};


		String DATA_HOME = "/home/sfathollahzadeh/GRTest/Datasets/Dataset2";
		String LOG_HOME ="/home/sfathollahzadeh/GRTest/Datasets/Dataset2/LOG/";

		Util u = new Util();
		int runs = 1;
		int nrows = 5000;
		int ncols = 5000;
		float sparsity = 1.0f;
		String fileName = "_nrows_" + nrows + "_ncols_" + ncols + "_sparsity_" + sparsity + ".raw";

		double mapping_time;
		double analysis_time;
		double read_time;
		long ram_nlines;
		int e=10;


		String head = "dataset,sparsity,data_nrows,data_ncols,matrix_size,mapping_time,analysis_time,read_time";
		for(String d : datasets) {
			boolean flagFile = false;
			String logFileName = LOG_HOME+"/synthetic-"+d+".csv";

			for(int i = 10; i <= 100; i+=10) {

				mapping_time = 0;
				analysis_time = 0;
				read_time = 0;

				for(int r = 0; r < runs; r++) {
					String dataFileName = DATA_HOME + "/data/" + d + fileName;
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
					else if(d.contains("MM-FZ-SYM") || d.contains("MM-FO-SYM")) {
						dn = "MM-SYM";
					}
					else if(d.contains("MM-FZ-SKEW") || d.contains("MM-FO-SKEW")) {
						dn = "MM-SKEW";
					}
					else if(d.contains("MM-FZ") || d.contains("MM-FO")) {
						dn = "MM";
					}
					String rawFileName = DATA_HOME + "/samples/" + d + "_nrows_" + i + "_ncols_" + i + "_sparsity_" + sparsity + ".raw";
					String sampleMatrixFileName = DATA_HOME + "/samples/" + dn + "_nrows_" + i + "_ncols_" + i + "_sparsity_" + sparsity + ".matrix";

					FileFormatPropertiesCSV csvpro = new FileFormatPropertiesCSV(false, ",", false);
					ReaderTextCSV csv = new ReaderTextCSV(csvpro);
					MatrixBlock sampleMatrix = csv
						.readMatrixFromHDFS(sampleMatrixFileName, row, col, -1, -1);

					String sampleRaw = u.readEntireTextFile(rawFileName);

					long tmpTime = System.nanoTime();
					GenerateReader.GenerateReaderMatrix gr = new GenerateReader.GenerateReaderMatrix(sampleRaw, sampleMatrix);
					mapping_time += (System.nanoTime() - tmpTime) / 1000000000.0;

					tmpTime = System.nanoTime();
					MatrixReader mr = gr.getReader();
					analysis_time += (System.nanoTime() - tmpTime) / 1000000000.0;

					tmpTime = System.nanoTime();
					MatrixBlock matrixBlock = mr.readMatrixFromHDFS(dataFileName, -1, clen, -1, -1);
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

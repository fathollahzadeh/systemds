package org.apache.sysds.test.functions.iogen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class dupData {

	public static void main(String[] args) throws IOException {

		int d=10;
		String inPath="/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/csv.data";
		String outPath="/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/csv/csv.data."+d;

		BufferedWriter writer = new BufferedWriter(new FileWriter(outPath));

		for(int i=0;i<d;i++) {
			try(BufferedReader br = new BufferedReader(new FileReader(inPath))) {
				String line;
				while((line = br.readLine()) != null) {
					writer.write(line);
					writer.write("\n");
				}
			}
			catch(FileNotFoundException e) {
				e.printStackTrace();
			}
			catch(IOException e) {
				e.printStackTrace();
			}
		}
		writer.flush();
		writer.close();
	}
}

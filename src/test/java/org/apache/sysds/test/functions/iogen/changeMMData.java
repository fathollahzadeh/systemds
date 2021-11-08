package org.apache.sysds.test.functions.iogen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class changeMMData {

	public static void main(String[] args) throws IOException {

		int d=1;
		String inPath="/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/mm/mm.data";
		String outPath="/media/sfathollahzadeh/Windows1/saeedData/FlatDatasets/mm/mm2.data."+d;

		BufferedWriter writer = new BufferedWriter(new FileWriter(outPath));
		for(int i=0;i<d;i++) {
			try(BufferedReader br = new BufferedReader(new FileReader(inPath))) {
				String line;

				while((line = br.readLine()) != null) {
					String[] l=line.split(" ");
					Integer I=Integer.parseInt(l[0]) +1;
					Integer J=Integer.parseInt(l[1]) + 1;
					String V=l[2];

					writer.write(I+" "+" "+J+" "+V);
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

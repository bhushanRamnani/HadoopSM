import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SimilarityMatrixReducer extends MapReduceBase implements
		Reducer<AFPWritable, Text, AFPWritable, Text> {
	
	Text distWritable = new Text();
	
	ArrayList<Coordinates> sequence1;
	ArrayList<Coordinates> sequence2;
	String pdb1;
	String pdb2;
	int i;
	int j;

	public final int M = 8;

	@Override
	public void reduce(AFPWritable key, Iterator<Text> values,
			OutputCollector<AFPWritable, Text> collector, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		sequence1 = new ArrayList<Coordinates>(M);
		sequence2 = new ArrayList<Coordinates>(M);

		String[] data1 = key.segment1.split(":");
		pdb1 = data1[0];
		i = Integer.parseInt(data1[1]);

		String[] data2 = key.segment2.split(":");
		pdb2 = data2[0];
		j = Integer.parseInt(data2[1]);

		// Extract Sequences
		while (values.hasNext()) {
			String residue = values.next().toString();
			String[] residueInfo = residue.split("\t");
			String pdbId = residueInfo[0];
			int resNo = Integer.parseInt(residueInfo[1]);
			double X = Double.parseDouble(residueInfo[5]);
			double Y = Double.parseDouble(residueInfo[6]);
			double Z = Double.parseDouble(residueInfo[7]);
			Coordinates coord = new Coordinates(X, Y, Z);
			if (pdbId.equals(pdb1)) {
				sequence1.set(resNo - i, coord);
			} else {
				sequence2.set(resNo - i, coord);
			}
		}

		// Calculate Distance
		double dist = calcDistance(sequence1, sequence2);
		
		distWritable.set(Double.toString(dist));
		collector.collect(key, distWritable);
	}

	public double calcDistance(ArrayList<Coordinates> sequence1,
			ArrayList<Coordinates> sequence2) {
		double dist = 0.0;
		for (int a = 0; a < M; a++) {
			for (int b = 0; b < M; b++) {
				double eucDist1 = getEucledian(sequence1.get(a), sequence1.get(b));
				double eucDist2 = getEucledian(sequence2.get(a), sequence2.get(b));
				dist += Math.abs(eucDist1 - eucDist2);
			}
		}
		dist = dist / (M * M);
		return dist;
	}

	public double getEucledian(Coordinates point1, Coordinates point2) {
		double a = Math.pow(point1.x - point2.x, 2)
				+ Math.pow(point1.y - point2.y, 2)
				+ Math.pow(point1.z - point2.z, 2);
		return Math.sqrt(a);
	}

}

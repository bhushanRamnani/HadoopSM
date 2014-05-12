import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * 
 * @author Bhushan Ramnani
 * 
 *         Iterative Algorithm to generate Similarity matrix of multiple protein
 *         files. Usage "java RunSM <input_folder>. The input folder contains
 *         all the pdb files. The output matrix will be generated in the file
 *         matrix.txt in the format <afp_id>\t<distance>
 */
public class RunSM {

	private static BufferedWriter brWrite;
	private static ArrayList<Protein> proteinList = new ArrayList<Protein>();
	private static final int M = 8;

	public static void generateProteinList(String input) throws IOException {
		File folder = new File(input);

		for (File pdbFile : folder.listFiles()) {
			String name = pdbFile.getName();
			String pdbId = name.substring(0, name.length() - 4);
			Protein protein = new Protein(pdbId);
			String line = null;

			BufferedReader br = new BufferedReader(new FileReader(pdbFile));
			while ((line = br.readLine()) != null) {
				if (line.substring(0, 4).equals("ATOM")) {
					String[] residueInfo = line.split("\\s+");
					if (residueInfo[2].equals("CA")) {
						double x = Double.parseDouble(residueInfo[6]);
						double y = Double.parseDouble(residueInfo[7]);
						double z = Double.parseDouble(residueInfo[8]);
						Coordinates point = new Coordinates(x, y, z);
						protein.addPoint(point);
					}
				}
			}
			proteinList.add(protein);
			System.out.println(pdbId + " length:" + protein.getPoints().size());
		}
	}

	public static void main(String[] args) {
		String input = args[0];
		try {
			long startTime = System.currentTimeMillis();
			System.out
					.println("Make sure you have an empty file called matrix.txt in this folder.");
			brWrite = new BufferedWriter(new FileWriter("matrix.txt"));
			generateProteinList(input);
			for (int i = 0; i < proteinList.size(); i++) {
				for (int j = i + 1; j < proteinList.size(); j++) {
					generateDistanceMatrix(proteinList.get(i),
							proteinList.get(j));
				}
			}
			System.out.println("Total Time="
					+ (System.currentTimeMillis() - startTime));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void generateDistanceMatrix(Protein a, Protein b)
			throws IOException {
		ArrayList<Coordinates> points1 = a.getPoints();
		ArrayList<Coordinates> points2 = b.getPoints();

		for (int i = 0; i <= points1.size() - M; i++) {
			for (int j = 0; j <= points2.size() - M; j++) {
				double dist = calcDistance(points1, i, points2, j);
				brWrite.write(a.getPdbId() + ":" + i + "," + b.getPdbId() + ":"
						+ j + "\t" + dist + "\n");
			}
		}

		System.out.println("Matrix written in matrix.txt");
	}

	public static double calcDistance(ArrayList<Coordinates> sequence1, int i,
			ArrayList<Coordinates> sequence2, int j) {
		double dist = 0.0;
		for (int a = 0; a < M; a++) {
			for (int b = 0; b < M; b++) {
				double eucDist1 = getEucledian(sequence1.get(i + a),
						sequence1.get(i + b));
				double eucDist2 = getEucledian(sequence2.get(j + a),
						sequence2.get(j + b));
				dist += Math.abs(eucDist1 - eucDist2);
			}
		}
		dist = dist / (M * M);
		return dist;
	}

	public static double getEucledian(Coordinates point1, Coordinates point2) {
		double a = Math.pow(point1.x - point2.x, 2)
				+ Math.pow(point1.y - point2.y, 2)
				+ Math.pow(point1.z - point2.z, 2);
		return Math.sqrt(a);
	}

}

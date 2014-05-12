import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

//import org.apache.hadoop.mapred.JobConf;

/**
 * Author: Bhushan Ramnani
 * 
 * This is the entry point for running the CE algorithm for multiple structure
 * alignment of two proteins. The algorithm takes place in two steps. 1)
 * Building the similarity matrix The pair of pdb files are given to a mapper
 * which divides it into a segment of length m and generates AFP (Aligned
 * Segment Pair). This AFP is then sent to a reducer which calculates the
 * distance and stores the pair as: <residue_no. A>, <residue_no. B>\t<distance>
 * 
 * 2) Extending the alignment
 * 
 * @param args
 */

public class RunCE {

	private final static String PROCESSED_INPUT_PATH = "processed_input";
	private final static File folder = new File(PROCESSED_INPUT_PATH);
	private static HashMap<String, Integer> proteinLengths = new HashMap<String, Integer>();

	public static void main(String[] args) {

		// Preprocess Input
		try {
			folder.mkdir();
			preprocess(args[0]);

			JobConf conf = new JobConf(RunCE.class);
			conf.setJobName("Similarity Matrix");
			conf.setOutputKeyClass(AFPWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(SimilarityMatrixMapper.class);
			conf.setReducerClass(SimilarityMatrixReducer.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, new Path(PROCESSED_INPUT_PATH));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));
			setProteinLengths(conf);
			JobClient.runJob(conf);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out
					.println("Either the file does not exist or the input path is not a folder.");
			e.printStackTrace();
		}
	}

	/**
	 * This function reads all the pdb files in the input folder and
	 * preprocesses each file. The preprocessed file gets stored in the
	 * processed input folder.
	 * 
	 * @param inputFolderPath
	 *            folder in which all the pdb files are stored
	 * @throws IOException
	 */
	private static void preprocess(String inputFolderPath) throws IOException {
		File dir = new File(inputFolderPath);
		if (!dir.isDirectory()) {
			throw new IOException("Input path needs to be a folder.");
		}
		for (File file : dir.listFiles()) {
			preprocessPdbFile(file);
		}
	}

	/**
	 * Preprocesses the pdb file (only keeps alpha carbon atoms) and stores it
	 * in the processed input folder in the following format:
	 * <pdb_id>\t<residue_no
	 * >\t<residue>\t<chain>\t<chain_no>\t<X-coordinate>\t<
	 * Y-coordinate>\t<Z-coordinate>
	 * 
	 * @param file
	 *            an input pdb file
	 * @throws IOException
	 */
	private static void preprocessPdbFile(File file) throws IOException {
		String fileName = file.getName();
		String pdbId = fileName.substring(0, fileName.length() - 4);
		File writeFile = new File(PROCESSED_INPUT_PATH + "/" + pdbId);
		writeFile.createNewFile();
		BufferedReader brIn = new BufferedReader(new FileReader(file));
		BufferedWriter brOut = new BufferedWriter(new FileWriter(writeFile));
		String line = null;
		int proteinLength = 0;
		while ((line = brIn.readLine()) != null) {
			if (line.substring(0, 4).equals("ATOM")) {
				String[] residueInfo = line.split("\\s+");
				if (residueInfo[2].equals("CA")) {
					proteinLength++;
					brOut.write(pdbId + "\t" + proteinLength + "\t"
							+ residueInfo[3] + "\t" + residueInfo[4] + "\t"
							+ residueInfo[5] + "\t" + residueInfo[6] + "\t"
							+ residueInfo[7] + "\t" + residueInfo[8] + "\n");
				}
			}
		}
		proteinLengths.put(pdbId, proteinLength);
		brIn.close();
		brOut.close();
	}

	/**
	 * Sets the protein lengths as a string in this format:
	 * <pdb_id1>:<l1>,<pdb_id>:<l2>,...
	 * 
	 * @param conf
	 */

	private static void setProteinLengths(JobConf conf) {
		StringBuffer stringBuffer = new StringBuffer();
		for (Map.Entry<String, Integer> entry : proteinLengths.entrySet()) {
			stringBuffer.append(entry.getKey());
			stringBuffer.append(":");
			stringBuffer.append(entry.getValue());
			stringBuffer.append(",");
		}
		conf.set("protein_lengths", stringBuffer.toString());
	}

}

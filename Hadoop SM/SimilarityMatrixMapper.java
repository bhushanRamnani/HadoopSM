import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * 
 * @author Bhushan Ramnani
 *	The mapper is responsible for sending the residue thrown at it to the appropriate reducer.
 */
public class SimilarityMatrixMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, AFPWritable, Text> {
	
	HashMap<String, Integer> proteinLengthMap;
	ArrayList<String> afpIds;
	private static final int M = 8;
	private AFPWritable afpWritable = new AFPWritable();
	private Text residueWritable = new Text();
	
	@Override
	public void configure(JobConf conf) {
		String proteinLengths=conf.get("protein_lengths");
		proteinLengthMap = new HashMap<String, Integer>();
		String[] lengths = proteinLengths.split(",");
		afpIds = new ArrayList<String>();
		for (String protein:lengths) {
			String[] data = protein.split(":");
			proteinLengthMap.put(data[0], Integer.parseInt(data[1]));
		}
		
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<AFPWritable, Text> collecter, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		String residue = value.toString();
		String[] residueInfo = residue.split("\t");
		String proteinId = residueInfo[0];
		int residueNo = Integer.parseInt(residueInfo[1]);
		
		for (int i = residueNo-(M-1); i<=residueNo; i++) {
			if (i>=1) {
				generateAFPIDs(proteinId, i);
			}
		}
		residueWritable.set(residue);
		for (String afpId:afpIds) {
			afpWritable.set(afpId);
			collecter.collect(afpWritable, residueWritable);
		}
	}
	
	private void generateAFPIDs(String pdbId, int i) {
		for (Map.Entry<String, Integer> entry:proteinLengthMap.entrySet()) {
			if (!entry.getKey().equals(pdbId)) {
				String pdbId2 = entry.getKey();
				int len = entry.getValue();
				for (int j=1; j<=len-M; j++) {
					String afpId = pdbId+":"+i+","+pdbId2+":"+j;
					afpIds.add(afpId);
				}
			}
		}
	}

}

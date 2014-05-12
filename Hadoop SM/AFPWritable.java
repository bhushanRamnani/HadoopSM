import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * This class represents the AFP. each segment in the pair is in the format
 * <pdbId:residueNumber>
 * 
 * @author Bhushan
 * 
 */
public class AFPWritable implements WritableComparable {

	String fullString;
	String segment1;
	String segment2;

	public void set(String input) throws IOException {
		String[] segments = input.split(",");
		if (segments.length < 2) {
			throw new IOException();
		}
		fullString = input;
		segment1 = segments[0];
		segment2 = segments[1];
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		fullString = input.readLine();
		String[] segments = fullString.split(",");
		if (segments.length < 2) {
			throw new IOException();
		}
		segment1 = segments[0];
		segment2 = segments[1];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeBytes(fullString);

	}

	@Override
	public int compareTo(Object other) {
		// TODO Auto-generated method stub
		if (other == null || !(other instanceof AFPWritable)) {
			return -1;
		}
		AFPWritable otherAFP = (AFPWritable) other;
		if ((otherAFP.segment1.equals(segment1) && otherAFP.segment2
				.equals(segment2))
				|| (otherAFP.segment1.equals(segment2) && otherAFP.segment2
						.equals(segment1))) {
			return 0;
		}
		return fullString.compareTo(otherAFP.fullString);
	}
	
	@Override
	public int hashCode() {
		return fullString.hashCode();
	}

}

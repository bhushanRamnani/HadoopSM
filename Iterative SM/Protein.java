import java.util.ArrayList;



/**
 * 
 * @author Bhushan Ramnani
 * Represents a protein
 */
public class Protein {

	private String pdbId;
	ArrayList<Coordinates> points;
	
	public Protein(String pdbId) {
		this.pdbId = pdbId;
		points = new ArrayList<Coordinates>();
	}
	
	public void addPoint(Coordinates point) {
		points.add(point);
	}
	
	public ArrayList<Coordinates> getPoints() {
		return points;
	}
	
	public String getPdbId() {
		return pdbId;
	}
}

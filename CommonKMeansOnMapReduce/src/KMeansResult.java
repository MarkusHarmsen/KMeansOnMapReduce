import java.util.ArrayList;

public class KMeansResult {
	public ArrayList<Point> centers;
	public double costs;
	public int iterations;
	public long duration = -1;
	
	public String toCSV() {
		return this.centers.size() + ";" + this.costs + ";" + this.iterations + ";" + (int)(this.duration / (1000 * 1000));
	}
}

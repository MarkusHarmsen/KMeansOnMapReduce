import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMeans__ {
	private final static float OVERSAMPLING_FACTOR = 0.5f;
	private final static boolean USE_WEIGHTED = true;

	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length != 1) {
			System.err.println("Usage: <k-clusters>");
			System.exit(-1);
		}

		KMeansResult result = run(Integer.parseInt(args[0]), OVERSAMPLING_FACTOR  * Integer.parseInt(args[0]), 5);
		ResultReaderHelper.printResults(result);
	}

	public static KMeansResult run(int kClusters, float oversampling, int maxRounds) throws IOException {
		Path input = new Path("input");
		Path cache = new Path("cache");

		// Prepare fs object for file reading
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(cache, true);
		fs.mkdirs(cache);

		// Select initial one center randomly
		ArrayList<Point> centers = ResultReaderHelper.getInitialCenters(fs, input, 1);
		// Costs of selected initial center:
		double initialCosts = KMeansPP.computeCostsAndCacheDistance(fs, input, centers, cache, false);
		int rounds = Math.min((int)Math.log(initialCosts), maxRounds);
		int selectedCenters = 0;

		// Now in log(costs) rounds do:
		for (int i = 0; i < rounds; i++) {
			System.out.println("Round: " + (i + 1) + "/" + rounds);
			double costs;

			// If this is the first run: costs did not change, so use them
			// again.
			if (initialCosts > 0) {
				costs = initialCosts;
				initialCosts = -1;
			} else {
				// I'm sorry - compute costs again
				costs = KMeansPP.computeCostsAndCacheDistance(fs, input, centers, cache, false);
			}

			// Select next centers
			ArrayList<Point> newCenters = selectNextCenters(fs, cache, costs, oversampling);
			if (newCenters.isEmpty()) {
				System.err.println("Warning: no centers selected in this round");
			} else {
				centers.addAll(newCenters);
				selectedCenters += newCenters.size();
			}
		}

		if (selectedCenters < kClusters) {
			System.err.println("Error: only " + selectedCenters + " instead of needed " + kClusters + " have been selected");
			return null;
		}

		/*
		 * We have now an expected number of log(costs) * OVERSAMPLING_FACTOR centers and must
		 * reduce them
		 */
		Path inputPP = new Path("input_pp");
		fs.delete(inputPP, true);
		fs.mkdirs(inputPP);

		// Write weighted centers
		ArrayList<Point> wCenters = weightCenters(fs, input, centers);
		writeWeightedCenters(fs, inputPP, wCenters);

		// Run K-Means++ on weighted clusters
		Path cachePP = new Path("cache_pp");
		KMeansResult result = KMeansPP.run(kClusters, inputPP, cachePP, USE_WEIGHTED, false);
		result.costs = ResultReaderHelper.computeCosts(fs, input, result.centers);

		return result;
	}

	public static ArrayList<Point> selectNextCenters(FileSystem fs, Path input, final double costs, final double oversampling) throws IOException {
		final Random rand = new Random();
		final ArrayList<Point> centers = new ArrayList<Point>();

		ResultReaderHelper.lineRunner(fs, input, new LineCallback() {
			@Override
			public void onLine(String line) {
				// Parse CSV line as point
				Point p = Point.parse(line);

				// Get saved distance
				float distance = p.pop();

				// Compute probability for x to be selected
				double probability = oversampling * distance / costs;

				// Do a random dice roll between 0 and 1/probability. If it is
				// zero (change should be probability), select it.
				if (probability >= 1 || rand.nextInt((int)(1 / probability)) == 0) {
					centers.add(p);
				}
			}
		});

		return centers;
	}

	/*
	 * Weight each center by counting the points closer to it than to any other
	 * center
	 */
	public static ArrayList<Point> weightCenters(FileSystem fs, Path input, final ArrayList<Point> centers) throws IOException {
		ResultReaderHelper.lineRunner(fs, input, new LineCallback() {
			@Override
			public void onLine(String line) {
				// Parse CSV line as point
				Point p = Point.parse(line);

				// Get nearest center
				int centerID = p.getNearestPoint(centers).index;
				centers.get(centerID).weight++;
			}
		});

		return new ArrayList<Point>(centers);
	}

	/*
	 * Write weighted centers to file
	 */
	public static void writeWeightedCenters(FileSystem fs, Path output, ArrayList<Point> centers) throws IOException {
		FSDataOutputStream out = fs.create(new Path(output.toUri().getPath() + "/weighted_centers.csv"));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		for (int i=0; i < centers.size(); i++) {
			if(USE_WEIGHTED) {
				try {
					bw.write(centers.get(i).toString(USE_WEIGHTED) + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				for (int j = centers.get(i).weight; j > 0; j--) {
					try {
						bw.write(centers.get(i).toString(USE_WEIGHTED) + "\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		bw.close();
		out.close();
	}
}

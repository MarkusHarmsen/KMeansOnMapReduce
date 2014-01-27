import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMeans {
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Usage: <k-clusters>");
			System.exit(-1);
		}

		KMeansResult result = run(Integer.parseInt(args[0]));
		ResultReaderHelper.printResults(result);
	}

	public static KMeansResult run(int kClusters) throws IOException {
		Path input = new Path("input");

		// Prepare fs object for file reading
		FileSystem fs = FileSystem.get(new Configuration());

		// Select initial k centers
		ArrayList<Point> centers = ResultReaderHelper.getInitialCenters(fs, input, kClusters);

		// Run
		KMeansResult result = KMeans.kMeans(centers, fs, input);
		result.costs = ResultReaderHelper.computeCosts(fs, input, result.centers);

		return result;
	}

	public static KMeansResult kMeans(final ArrayList<Point> centers, FileSystem fs, Path input) throws IOException {
		return kMeans(centers, fs, input, false);
	}

	public static KMeansResult kMeans(final ArrayList<Point> centers, FileSystem fs, Path input, final boolean weighted) throws IOException {
		ArrayList<Point> oldCenters = null;
		int iterations = 0;

		// Iteration loop: while centers change
		while (!centers.equals(oldCenters)) {
			oldCenters = new ArrayList<Point>(centers);
			iterations++;

			// Use array instead of array list, otherwise the list must be
			// initialized before
			final Point pSum[] = new Point[centers.size()];

			ResultReaderHelper.lineRunner(fs, input, new LineCallback() {
				@Override
				public void onLine(String line) {
					// Parse CSV line as point
					Point p = Point.parse(line, weighted);

					// Get nearest center
					int id = p.getNearestPoint(centers).index;
					p.append(1);

					// Sum points
					for (int i = 0; i < p.weight; i++) {
						if (pSum[id] == null) {
							// Create new point here, otherwise we would do
							// p^p.weight (since it is the same object) in the
							// next steps when weight > 1
							pSum[id] = new Point(p);
						} else {
							pSum[id].add(p);
						}
					}
				}
			});

			// Divide
			for (int i = 0; i < pSum.length; i++) {
				if (pSum[i] == null) {
					System.err.println("Error: lost one cluster during iteration");
				}
				int size = (int) pSum[i].pop();
				pSum[i].divideBy(size);
			}

			centers.clear();
			centers.addAll(Arrays.asList(pSum));
		}

		KMeansResult result = new KMeansResult();
		result.iterations = iterations;
		result.centers = centers;

		return result;
	}
}

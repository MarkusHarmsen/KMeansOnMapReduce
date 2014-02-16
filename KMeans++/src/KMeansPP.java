import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMeansPP {
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
		Path cache = new Path("cache");
		return run(kClusters, input, cache, false, true);
	}

	public static KMeansResult run(int kClusters, Path input, Path cache, boolean weighted, boolean computeMad) throws IOException {
		// Prepare fs object for file reading
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(cache, true);
		fs.mkdirs(cache);

		// Select initial one center randomly
		ArrayList<Point> centers = ResultReaderHelper.getInitialCenters(fs, input, 1, weighted);
		
		// Select remaining centers
		for (int i = 1; i < kClusters; i++) {
			double distanceSum = computeCostsAndCacheDistance(fs, input, centers, cache, weighted);
			centers.add(selectNextCenter(fs, cache, distanceSum, weighted));
			//System.out.println("Cluster selected: " + i);
		}

		// Run "classic" K-Means
		KMeansResult result = KMeans.kMeans(centers, fs, input, weighted);
		if (computeMad) {
			result.costs = ResultReaderHelper.computeCosts(fs, input, result.centers);
		}

		return result;
	}

	public static double computeCostsAndCacheDistance(FileSystem fs, Path input, final ArrayList<Point> centers, Path cache, final boolean weighted) throws IOException {
		// Use ArrayList as wrapper
		final ArrayList<Double> sum = new ArrayList<Double>(1);
		sum.add(0.0);

		final BufferedWriter bw;

		// Setup cache file
		FSDataOutputStream out = fs.create(new Path(cache.toUri().getPath() + "/point_w_distances.csv"));
		bw = new BufferedWriter(new OutputStreamWriter(out));

		ResultReaderHelper.lineRunner(fs, input, new LineCallback() {
			@Override
			public void onLine(String line) {
				// Parse CSV line as point
				Point p = Point.parse(line, weighted);

				NearestPointStats stats = p.getNearestPoint(centers);

				float distance = stats.distance;
				if (weighted) {
					distance *= p.weight;
				}

				sum.set(0, sum.get(0) + distance);

				// Write to cache file
				p.append(distance);
				try {
					bw.write(p.toString(weighted) + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		bw.close();
		out.close();

		return sum.get(0);
	}

	public static Point selectNextCenter(FileSystem fs, Path input, double costs, final boolean weighted) throws IOException {
		// Random select a value between 0 and distanceSum
		final double sel = new Random().nextDouble() * costs;

		// Use ArrayList as wrapper
		final ArrayList<Double> sum = new ArrayList<Double>(1);
		sum.add(0.0);

		final ArrayList<Point> center = new ArrayList<Point>(1);

		ResultReaderHelper.lineRunner(fs, input, new LineCallback() {
			@Override
			public void onLine(String line) {
				// Parse CSV line as point
				Point p = Point.parse(line, weighted);

				// Get saved distance and add to overall current sum
				double currSum = sum.get(0) + p.pop();
				sum.set(0, currSum);

				/*
				 * If sum is bigger than the selected one: choose that element,
				 * this should result in a weighted element selection
				 */
				if (currSum > sel) {
					this.cancel = true; // Cancel loop
					center.add(p);
				}
			}
		});

		return center.get(0);
	}
}

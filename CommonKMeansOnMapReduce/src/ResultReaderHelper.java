import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultReaderHelper {
	/*
	 * Select randomly k initial centers from given path. Due to the selection process, the first and last line may have a lower chance to be selected, but I
	 * don't care ;) Furthermore this crappy selection will not determine if not at least k lines are in the input file(s)
	 */
	public static ArrayList<Point> getInitialCenters(FileSystem fs, Path input, int k) throws IOException {
		return getInitialCenters(fs, input, k, false);
	}

	public static ArrayList<Point> getInitialCenters(FileSystem fs, Path input, int k, boolean weighted) throws IOException {
		FileStatus status[] = fs.listStatus(input);
		ArrayList<Point> centers = new ArrayList<Point>(k);

		Random rand = new Random();

		// Until we have not reached k centers
		while (centers.size() < k) {
			// Initialize buffer here - I had some trouble with \0 bytes when doing this outside the loop
			byte[] buffer = new byte[1024];

			// Open random file (if there is more than one)
			int file = rand.nextInt(status.length);
			if (!fs.isFile(status[file].getPath())) {
				continue;
			}
			FSDataInputStream is = fs.open(status[file].getPath());

			// Get available bytes and choose a random position inside the file
			int available = is.available();
			int pos = rand.nextInt(available + 1);
			int count = Math.min(buffer.length, available - pos);

			// Skip empty files
			if (available == 0) {
				continue;
			}

			// Read buffer (with maximal length) at random position and get lines
			is.readFully(pos, buffer, 0, count);
			String lines[] = new String(buffer, "UTF-8").split("\n");

			// Skip if no newline was found
			if (lines.length == 1) {
				continue;
			}

			// Close file
			is.close();

			// Edge cases
			// if pos is 0 (beginning of file), the first element is allowed
			int min = pos == 0 ? 0 : 1;
			// if the read data is lower than the buffer (end of file), the last element is allowed
			int max = count < buffer.length ? lines.length - 1 : lines.length - 2;
			int sel = rand.nextInt(max + 1 - min) + min;

			String line = lines[sel].trim();
			if (line.isEmpty()) {
				// Skip if that was a empty line
				continue;
			}

			Point point = Point.parse(line, weighted);

			// If this line has not been selected before: add to selected centers
			if (!centers.contains(point)) {
				centers.add(point);
			}
		}

		return centers;
	}

	public static String arrayListPointsToString(ArrayList<Point> points) {
		StringBuilder sb = new StringBuilder();
		for (Point p : points) {
			sb.append(p.toString());
			sb.append("\n");
		}
		return sb.toString().trim();
	}

	/*
	 * Read in computed centers from output directory (each file in the directory is used).
	 */
	private static final Pattern linePattern = Pattern.compile("^([0-9]+)\\s+(.*)$");
	public static String getCenters(FileSystem fs, Path input, final boolean withIndex) throws IOException {
		final TreeMap<Integer, String> centers = new TreeMap<Integer, String>();
		final ArrayList<Integer> index = new ArrayList<Integer>(1);
		index.add(0);
		
		ResultReaderHelper.lineRunner(fs, input, new LineCallback() {
			@Override
			public void onLine(String line) {
				if(withIndex) {
					/*
					 * Match with index
					 */
					Matcher m = linePattern.matcher(line);
					if(m.matches()) {
						centers.put(Integer.valueOf(m.group(1)), m.group(2));
					}
				} else {
					/*
					 * No index in file -> create one since order seems not to be important
					 */
					centers.put(index.get(0), line);
					index.set(0, index.get(0) + 1);
				}
			}
		});

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < centers.size(); i++) {
			sb.append(centers.get(i));
			sb.append("\n");
		}
		return sb.toString().trim();
	}

	/*
	 * For given centers: compute costs
	 */
	public static double computeCosts(FileSystem fs, Path input, final ArrayList<Point> centers) throws IOException {
		final ArrayList<Double> sum = new ArrayList<Double>(1);
		sum.add(0.0);

		ResultReaderHelper.lineRunner(fs, input, new LineCallback() {
			@Override
			public void onLine(String line) {
				// Parse CSV line as point
				Point p = Point.parse(line);
				NearestPointStats stats = p.getNearestPoint(centers);
				sum.set(0, sum.get(0) + stats.distance);
			}
		});

		return sum.get(0);
	}

	/*
	 * Open all files in input path and call for each line read in the callback "onLine"
	 */
	public static void lineRunner(FileSystem fs, Path input, LineCallback callback) throws IOException {
		// Open each input file
		FileStatus status[] = fs.listStatus(input);
		for (int i = 0; i < status.length; i++) {
			Path current = status[i].getPath();
			if (!fs.isFile(current)) {
				continue;
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(current)));
			String line;
			while ((line = br.readLine()) != null) {
				callback.onLine(line);

				// Cancel if desired
				if (callback.cancel) {
					break;
				}
			}
			br.close();
		}
	}

	/*
	 * Compute "equality" of two Point lists
	 */
	/*
	public static float equalityOfPoints(ArrayList<Point> a, ArrayList<Point> b) {
		if (a == null || b == null || a.size() != b.size()) {
			return -1;
		}
		float sum = 0;
		for (int i = 0; i < a.size(); i++) {
			sum += a.get(i).squared_euclidean_distance(b.get(i));
		}
		return sum;
	}
	*/

	/*
	 * Print result
	 */
	public static void printResults(KMeansResult result) {
		System.out.println("Iterations: " + result.iterations);
		System.out.println("Costs: " + result.costs);
		// if(result.centers != null) {
		// System.out.println(result.centers);
		// }
		if (result.duration >= 0) {
			System.out.println("Duration: " + result.duration / (1000 * 1000) + "ms");
		}
	}
}

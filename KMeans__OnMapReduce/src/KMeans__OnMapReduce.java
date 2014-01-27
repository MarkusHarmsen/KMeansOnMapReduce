import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class KMeans__OnMapReduce {
	private final static float OVERSAMPLING_FACTOR = 0.5f;
	
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
		Path output = new Path("output");
		
		// Prepare fs object for file manipulation
		FileSystem fs = FileSystem.get(new Configuration());

		// Select initial one center randomly
		String centers = ResultReaderHelper.arrayListPointsToString(ResultReaderHelper.getInitialCenters(fs, input, 1));

		// Costs of selected initial center:
		double initialCosts = KMeans__Costs.computeCosts(input, centers);
		int rounds = (int) Math.log(initialCosts);
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
				costs = KMeans__Costs.computeCosts(input, centers);
			}
			
			// Create conf
			JobConf conf = createJob(input, output, i);

			// Set centers and costs for mappers
			conf.set("centers", centers);
			conf.setFloat("costs", (float) costs);
			conf.setFloat("oversampling", OVERSAMPLING_FACTOR * kClusters);

			// Remove old output directory
			fs.delete(output, true);

			// Go for it!
			JobClient.runJob(conf);

			// Get selected new centers
			String newCenters = ResultReaderHelper.getCenters(fs, output, false);
			if (newCenters.isEmpty()) {
				System.err.println("Warning: no centers selected in this round");
			} else {
				centers += "\n" + newCenters;
				selectedCenters += newCenters.split("\n").length;
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

		// Write weighted centers (this will create the inputPP directory)
		KMeans__Weighting.weightCenters(input, inputPP, centers);

		// Run K-Means++ on weighted clusters (way smaller, so use KMeans++ on a
		// single machine)
		Path cachePP = new Path("cache_pp");
		KMeansResult result = KMeansPP.run(kClusters, inputPP, cachePP, true, false);
		result.costs = ResultReaderHelper.computeCosts(fs, input, result.centers);

		return result;
	}
	
	/*
	 * Create job
	 */
	private static JobConf createJob(Path input, Path output, int iteration) {
		// Create job configuration
		JobConf conf = new JobConf(KMeans__OnMapReduce.class);
		conf.setJobName("K-Means|| (" + iteration + ")");

		// Configure Mapper, a reducer is not needed
		conf.setMapperClass(KMeans__OnMapReduceMapper.class);
		conf.setNumReduceTasks(0);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(FloatArrayWritable.class);

		// Set input/output path
		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, output);
		
		return conf;
	}
}

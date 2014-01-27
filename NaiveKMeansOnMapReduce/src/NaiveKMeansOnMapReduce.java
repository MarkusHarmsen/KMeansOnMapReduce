import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class NaiveKMeansOnMapReduce {
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

		// Select initial k centers
		ArrayList<Point> oldCenters = null;
		ArrayList<Point> centers = ResultReaderHelper.getInitialCenters(fs, input, kClusters);
		int iterations = 0;

		// Iteration loop: while centers change
		while (!centers.equals(oldCenters)) {
			System.out.println("Iteration: " + (iterations + 1));
			//System.out.println("Equality: " + ResultReaderHelper.equalityOfPoints(centers, oldCenters));
			oldCenters = centers;
			iterations++;
			
			// Create conf
			JobConf conf = createJob(input, output, iterations);

			// Set new centers for mappers
			conf.set("centers", ResultReaderHelper.arrayListPointsToString(centers));

			// Remove old output directory
			fs.delete(output, true);

			// Go for it!
			JobClient.runJob(conf);

			// Get new centers
			centers = Point.parseAll(ResultReaderHelper.getCenters(fs, output, true));
		}
		
		KMeansResult result = new KMeansResult();
		result.centers = centers;
		result.iterations = iterations;
		result.costs = ResultReaderHelper.computeCosts(fs, input, result.centers);
		
		return result;
	}
	
	/*
	 * Create job
	 */
	private static JobConf createJob(Path input, Path output, int iteration) {
		// Create job configuration
		JobConf conf = new JobConf(NaiveKMeansOnMapReduce.class);
		conf.setJobName("K-Means naive (" + iteration + ")");

		// Set input/output path
		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, output);

		// Configure Mapper and Reducer
		conf.setMapperClass(NaiveKMeansOnMapReduceMapper.class);
		conf.setReducerClass(NaiveKMeansOnMapReduceReducer.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(FloatArrayWritable.class);
		
		return conf;
	}
}

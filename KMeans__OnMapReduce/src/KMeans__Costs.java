import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class KMeans__Costs {
	public static double computeCosts(Path input, String centers) throws IOException {
		// Create job configuration
		JobConf conf = new JobConf(KMeans__Costs.class);
		conf.setJobName("K-Means|| Cost computation");
		
		Path cache = new Path("cache_costs");
		
		// Prepare fs object for file manipulation
		FileSystem fs = FileSystem.get(conf);
		
		// Remove old output directory
		fs.delete(cache, true);

		// Set input/output path
		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, cache);
		
		// Configure Mapper and Reducer
		conf.setMapperClass(KMeans__CostsMapper.class);
		conf.setReducerClass(KMeans__CostsReducer.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		// Set centers for mappers
		conf.set("centers", centers);

		// Go for it!
		JobClient.runJob(conf);
		
		// Return computed costs
		String value = ResultReaderHelper.getCenters(fs, cache, false);
		
		return Double.parseDouble(value);
	}
}

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class KMeans__Weighting {
	public static void weightCenters(Path input, Path output, String centers) throws IOException {
		// Create job configuration
		JobConf conf = new JobConf(KMeans__Weighting.class);
		conf.setJobName("K-Means|| Weight computation");

		// Set input/output path
		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, output);
		
		// Configure Mapper and Reducer
		conf.setMapperClass(KMeans__WeightingMapper.class);
		conf.setReducerClass(KMeans__WeightingReducer.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);
		
		// Set centers for mappers
		conf.set("centers", centers);

		// Go for it!
		JobClient.runJob(conf);
	}
}

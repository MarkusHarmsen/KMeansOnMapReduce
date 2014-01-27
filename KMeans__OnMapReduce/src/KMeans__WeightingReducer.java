import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KMeans__WeightingReducer extends MapReduceBase implements Reducer<LongWritable, LongWritable, LongWritable, FloatArrayWritable> {
	private ArrayList<Point> centers;

	@Override
	/*
	 * Configure
	 */
	public void configure(JobConf job) {
		// Retrieve centers, shared by all mappers
		this.centers = Point.parseAll(job.get("centers"));
	}

	@Override
	/*
	 * Reduce
	 */
	public void reduce(LongWritable key, Iterator<LongWritable> values, OutputCollector<LongWritable, FloatArrayWritable> output, Reporter reporter)
			throws IOException {
		// Sum up from mappers
		long hits = 0;
		while (values.hasNext()) {
			hits += values.next().get();
		}
		
		Point weightedCenter = new Point(this.centers.get((int)key.get()));
		
		// Append weight as last value
		weightedCenter.append(hits);
		
		// Export weighted center point as array
		FloatArrayWritable export = new FloatArrayWritable(weightedCenter.getData());
		output.collect(null, export);
	}
}

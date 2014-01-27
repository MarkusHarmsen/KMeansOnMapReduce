import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class NaiveKMeansOnMapReduceMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, FloatArrayWritable> {
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
	 * Map
	 */
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, FloatArrayWritable> output, Reporter reporter) throws IOException {
		// Parse CSV line as point
		Point p = Point.parse(value.toString());

		// Use cluster ID as key
		key = new LongWritable(p.getNearestPoint(this.centers).index);

		// Export points as array
		FloatArrayWritable export = new FloatArrayWritable(p.getData());
		output.collect(key, export);
	}
}

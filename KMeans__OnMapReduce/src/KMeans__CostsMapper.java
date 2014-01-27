import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KMeans__CostsMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
	private ArrayList<Point> centers;
	private OutputCollector<LongWritable, DoubleWritable> output;
	private double costs;

	@Override
	/*
	 * Configure
	 */
	public void configure(JobConf job) {
		// Retrieve centers, shared by all mappers
		this.centers = Point.parseAll(job.get("centers"));
		this.costs = 0;
	}

	@Override
	/*
	 * Map
	 */
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, DoubleWritable> output, Reporter reporter) throws IOException {
		// Save collector reference
		this.output = output;

		// Parse CSV line as point
		Point p = Point.parse(value.toString());

		// Sum up distance to nearest center
		this.costs += p.getNearestPoint(this.centers).distance;
	}
	
	@Override
	/*
	 * Close
	 */
	public void close() throws IOException {
		// Emit costs
		DoubleWritable export = new DoubleWritable(this.costs);
		this.output.collect(new LongWritable(0), export);
	}

}

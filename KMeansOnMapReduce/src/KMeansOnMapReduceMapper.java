import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class KMeansOnMapReduceMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, FloatArrayWritable> {
	private ArrayList<Point> centers;
	private Point[] sum;

	private OutputCollector<LongWritable, FloatArrayWritable> output;

	@Override
	/*
	 * Configure
	 */
	public void configure(JobConf job) {
		// Retrieve centers, shared by all mappers
		this.centers = Point.parseAll(job.get("centers"));
		// Setup sum array, one for each center
		this.sum = new Point[this.centers.size()];
	}

	@Override
	/*
	 * Map
	 */
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, FloatArrayWritable> output, Reporter reporter) throws IOException {
		// Save collector reference
		this.output = output;

		// Parse CSV line as point
		Point p = Point.parse(value.toString());

		// Get nearest center
		int id = p.getNearestPoint(this.centers).index;

		// Append counting value to point. Otherwise the amount information
		// would be lost.
		p.append(1);

		// Sum points
		if (this.sum[id] == null) {
			this.sum[id] = p;
		} else {
			this.sum[id].add(p);
		}
	}

	@Override
	/*
	 * Close
	 */
	public void close() throws IOException {
		// Emit collected sum points
		for (int i = 0; i < this.sum.length; i++) {
			if(this.sum[i] == null) {
				continue;
			}
			FloatArrayWritable export = new FloatArrayWritable(this.sum[i].getData());
			this.output.collect(new LongWritable(i), export);
		}
	}
}

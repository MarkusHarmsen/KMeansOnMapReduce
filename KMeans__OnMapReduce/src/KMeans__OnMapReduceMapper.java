import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class KMeans__OnMapReduceMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, FloatArrayWritable> {
	private ArrayList<Point> centers;
	private ArrayList<Point> newCenters;
	private Random rand;
	private double costs;
	private float oversampling;

	private OutputCollector<LongWritable, FloatArrayWritable> output;

	@Override
	/*
	 * Configure
	 */
	public void configure(JobConf job) {
		// Retrieve centers, shared by all mappers
		this.centers = Point.parseAll(job.get("centers"));
		this.newCenters = new ArrayList<Point>();
		this.rand = new Random();
		this.costs = job.getFloat("costs", 0);
		this.oversampling = job.getFloat("oversampling", 0);
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

		// Compute probability
		double probability = oversampling * p.getNearestPoint(this.centers).distance / costs;

		// Do a random dice roll between 0 and 1/probability. If it is
		// zero (change should probability), select it.
		if (probability > 1 || rand.nextInt(((int) Math.ceil(1 / probability))) == 0) {
			newCenters.add(p);
		}
	}

	@Override
	/*
	 * Close
	 */
	public void close() throws IOException {
		// Emit collected new Centers
		for (int i = 0; i < this.newCenters.size(); i++) {
			FloatArrayWritable export = new FloatArrayWritable(this.newCenters.get(i).getData());
			this.output.collect(null, export);
		}
	}
}

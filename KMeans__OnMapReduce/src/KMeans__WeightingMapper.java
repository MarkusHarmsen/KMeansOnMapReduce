import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class KMeans__WeightingMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, LongWritable> {
	private ArrayList<Point> centers;
	private long hits[];
	private OutputCollector<LongWritable, LongWritable> output;
	
	@Override
	/*
	 * Configure
	 */
	public void configure(JobConf job) {
		// Retrieve centers, shared by all mappers
		this.centers = Point.parseAll(job.get("centers"));
		this.hits = new long[this.centers.size()];
	}
	
	@Override
	/*
	 * Map
	 */
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, LongWritable> output, Reporter reporter) throws IOException {
		// Save collector reference
		this.output = output;
		
		// Parse CSV line as point
		Point p = Point.parse(value.toString());
		
		// Increase hits for nearest cluster
		this.hits[p.getNearestPoint(this.centers).index]++;
	}
	
	@Override
	/*
	 * Close
	 */
	public void close() throws IOException {
		// Emit costs
		for(int i=0; i < this.hits.length; i++) {
			this.output.collect(new LongWritable(i), new LongWritable(this.hits[i]));
		}
	}
}

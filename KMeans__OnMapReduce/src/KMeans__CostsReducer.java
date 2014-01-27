import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KMeans__CostsReducer extends MapReduceBase implements Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
	@Override
	/*
	 * Reduce
	 */
	public void reduce(LongWritable key, Iterator<DoubleWritable> values, OutputCollector<LongWritable, DoubleWritable> output, Reporter reporter)
			throws IOException {
		double costs = 0;
		
		// Sum up from mappers
		while (values.hasNext()) {
			costs += values.next().get();
		}
		
		// Emit costs
		DoubleWritable export = new DoubleWritable(costs);
		output.collect(null, export);
	}
}

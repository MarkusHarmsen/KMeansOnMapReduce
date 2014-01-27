import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NaiveKMeansOnMapReduceReducer extends MapReduceBase implements Reducer<LongWritable, FloatArrayWritable, LongWritable, FloatArrayWritable> {

	@Override
	/*
	 * Reduce
	 */
	public void reduce(LongWritable key, Iterator<FloatArrayWritable> values, OutputCollector<LongWritable, FloatArrayWritable> output, Reporter reporter)
			throws IOException {
		// First point as initial sum point. I assume a reducer is only called,
		// if values are not empty ;)
		Point sum = values.next().asPoint();
		int size = 1;

		// Sum up all remaining points
		while (values.hasNext()) {
			sum.add(values.next().asPoint());
			size++;
		}

		// Divide each value by size
		sum.divideBy(size);

		// Export new center point as array
		FloatArrayWritable export = new FloatArrayWritable(sum.getData());
		output.collect(key, export);
	}

}

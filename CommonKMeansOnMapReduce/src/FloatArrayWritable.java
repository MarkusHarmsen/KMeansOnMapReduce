import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class FloatArrayWritable extends ArrayWritable {

	public FloatArrayWritable() {
		super(FloatWritable.class);
	}

	public FloatArrayWritable(FloatWritable[] values) {
		super(FloatWritable.class, values);
	}

	/*
	 * Create from given ArrayList
	 */
	public FloatArrayWritable(ArrayList<Float> array) {
		this();
		
		FloatWritable values[] = new FloatWritable[array.size()];
		for (int i = 0; i < array.size(); i++) {
			values[i] = new FloatWritable(array.get(i));
		}
		
		this.set(values);
	}

	/*
	 * Export FloatArrayWritable to Point
	 */
	public Point asPoint() {
		return new Point(this.getData());
	}

	@Override
	public String toString() {
		String strings[] = super.toStrings();

		StringBuilder sb = new StringBuilder();
		for(int i=0; i < strings.length - 1; i++) {
			sb.append(strings[i]);
			sb.append(", ");
		}
		if(strings.length > 0) {
			sb.append(strings[strings.length - 1]);
		}
		return sb.toString();
	}
	
	/*
	 * Get data as float array
	 */
	private ArrayList<Float> getData() {
		Writable[] array = this.get();
		ArrayList<Float> values = new ArrayList<Float>(array.length);
		for (int i = 0; i < array.length; i++) {
			values.add(((FloatWritable) array[i]).get());
		}
		
		return values;
	}
}

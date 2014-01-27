import java.util.ArrayList;
import java.util.Iterator;

public class Point {
	private ArrayList<Float> values;
	public int weight = 1;

	public Point() {
		this.values = new ArrayList<Float>();
	}
	
	public Point(Point p) {
		this(new ArrayList<Float>(p.values), p.weight);
	}

	public Point(ArrayList<Float> values) {
		this(values, 1);
	}

	public Point(ArrayList<Float> values, int weight) {
		this.values = values;
		this.weight = weight;
	}

	@Override
	public boolean equals(Object o) {
		Point p = (Point) o;
		if (this.weight != p.weight) {
			return false;
		}
		if (!this.values.equals(p.values)) {
			return false;
		}
		return true;
	}

	/*
	 * Get single value
	 */
	public float get(int index) {
		return this.values.get(index);
	}

	/*
	 * Get internal data
	 */
	public ArrayList<Float> getData() {
		return this.values;
	}

	/*
	 * Append a value to a point
	 */
	public void append(float value) {
		this.values.add(value);
	}

	/*
	 * Pop last value
	 */
	public float pop() {
		return this.values.remove(this.values.size() - 1);
	}

	/*
	 * Add another point to current point. The values are summed up pairwise
	 * (vector addition).
	 */
	public void add(Point p) {
		for (int i = 0; i < this.values.size(); i++) {
			this.values.set(i, this.values.get(i) + p.values.get(i));
		}
	}

	/*
	 * Divide each value by given value
	 */
	public void divideBy(int value) {
		for (int i = 0; i < this.values.size(); i++) {
			this.values.set(i, this.values.get(i) / value);
		}
	}

	/*
	 * Compute distance to given point
	 */
	public float squared_euclidean_distance(Point p) {
		float sum = 0, val;
		for (int i = 0, size = this.values.size(); i < size; i++) {
			val = this.values.get(i) - p.get(i);
			sum += val * val;	// x * x is way faster than Math.pow(x, 2)
		}
		return sum;
	}

	/*
	 * Get the index of the nearest point in points to point p
	 */
	public NearestPointStats getNearestPoint(ArrayList<Point> points) {
		NearestPointStats result = new NearestPointStats();

		for (int i = 0, size = points.size(); i < size; i++) {
			float distance = this.squared_euclidean_distance(points.get(i));
			if (distance < result.distance) {
				result.distance = distance;
				result.index = i;
			}
		}

		return result;
	}

	@Override
	public String toString() {
		return toString(false);
	}

	public String toString(boolean weighted) {
		StringBuilder sb = new StringBuilder();

		Iterator<Float> it = this.values.iterator();
		while (it.hasNext()) {
			sb.append(it.next());
			if (it.hasNext()) {
				sb.append(",");
			}
		}
		
		// Add weight if desired
		if (weighted) {
			sb.append(",");
			sb.append((float)this.weight);
		}
		
		return sb.toString();
	}

	/*
	 * Parse single point from string line
	 */
	public static Point parse(String line) {
		return parse(line, false);
	}

	/*
	 * Parse single weighted point from string line
	 */
	public static Point parse(String line, boolean weighted) {
		int weight = 1;
		String[] values = line.split(",");
		int length = values.length;

		if (weighted) {
			// Use last value as weight
			weight = (int) Float.parseFloat(values[values.length - 1]);
			length--;
		}

		// Parse
		ArrayList<Float> fValues = new ArrayList<Float>(length);
		for (int i = 0; i < length; i++) {
			fValues.add(Float.parseFloat(values[i]));
		}

		return new Point(fValues, weight);
	}

	/*
	 * Parse points from string lines
	 */
	public static ArrayList<Point> parseAll(String lines) {
		return parseAll(lines, false);
	}
	
	/*
	 * Parse points weighted from string lines
	 */
	public static ArrayList<Point> parseAll(String lines, boolean weighted) {
		String[] values = lines.split("\n");
		ArrayList<Point> points = new ArrayList<Point>(values.length);
		for (int i = 0; i < values.length; i++) {
			points.add(Point.parse(values[i], weighted));
		}

		return points;
	}
}

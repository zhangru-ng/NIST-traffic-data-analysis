package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DayReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double speed = 0;
		double flow = 0;
		double occupancy = 0;
		int count = 0;
		for (Text v : values) {
			if (!v.toString().contains("-")) {
				String[] info = v.toString().split(",");
				speed += Double.parseDouble(info[0]);
				flow += Double.parseDouble(info[1]);
				occupancy += Double.parseDouble(info[2]);
				count++;
			}
		}

		context.write(key, new Text((speed / count) + "," + (flow / count)
				+ "," + (occupancy / count)));

	}
}

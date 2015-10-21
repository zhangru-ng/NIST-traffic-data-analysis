package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LaneDetectorReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int speed = 0;
		int flow = 0;
		int occupancy = 0;
		int count = 0;
		for (Text v : values) {
			if (!v.toString().contains("-")) {
				String[] info = v.toString().split(",");
				speed += Integer.parseInt(info[0]);
				flow += Integer.parseInt(info[1]);
				occupancy += Integer.parseInt(info[2]);
				count++;
			}
		}

		context.write(key, new Text((speed / (double) count) + ","
				+ (flow) + "," + (occupancy / (double) count)));

	}
}

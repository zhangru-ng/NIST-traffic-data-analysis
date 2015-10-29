package edu.ufl.ds.feature_extraction;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MonthReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try {
			double speed = 0;
			double flow = 0;
			double occupancy = 0;
			int count = 0;
			for (Text v : values) {
				String[] input = v.toString().split("\t");
				String[] info = input[0].split(",");

				double tempFlow = Double.parseDouble(info[3]);

				if (input[1].equals("0")) {
					tempFlow = Double.parseDouble(input[2]);
				}

				flow += tempFlow;

				if (!info[2].trim().startsWith("-"))
					speed += Double.parseDouble(info[2]);
				if (!info[4].trim().startsWith("-"))
					occupancy += Double.parseDouble(info[4]);
				count++;
			}

			context.write(key, new Text((speed / count) + "," + (flow / count)
					+ "," + (occupancy / count)));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}

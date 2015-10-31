package edu.ufl.ds.feature_extraction;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MonthReducer extends Reducer<Text, Text, Text, Text> {
	private Text outputKey = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try {
			System.out.println(key.toString());
			int count = 0;
			int[] event_counts = new int[6];
			for (Text v : values) {
				String temp[] = v.toString().split(",");
				// 38.7773,-77.0232,38.7948744154,-77.0059457656,2003-12,4,0,0,0,4,4

				for (int i = 0; i < 6; i++) {
					event_counts[i] += Double.parseDouble(temp[i + 5]);
				}
				count++;
			}
			String ss = "";
			for (int i = 0; i < 6; i++) {
				ss += "," + (event_counts[i] / count);
			}
			outputKey.set(key + ss);
			context.write(outputKey, null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}

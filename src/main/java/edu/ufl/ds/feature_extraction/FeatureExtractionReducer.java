package edu.ufl.ds.feature_extraction;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FeatureExtractionReducer extends
		Reducer<Text, Text, Text, NullWritable> {
	private Text outputKey = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, Integer> map = new HashMap<>();
		map.put("accidentsAndIncidents", 0);
		map.put("roadwork", 1);
		map.put("precipitation", 2);
		map.put("deviceStatus", 3);
		map.put("obstruction", 4);
		map.put("trafficConditions", 5);
		map.put("speed", 6);
		map.put("flow", 7);
		map.put("occupancy", 8);
		int[] counts = new int[map.size()];
		for (Text v : values) {
			String temp[] = v.toString().split(",");
			if (map.containsKey(temp[0])) {
				counts[map.get(temp[0])]++;
			}
		}
		String[] keyParts = key.toString().split(":");
		String ss = "";
		for (int i = 0; i < 6; i++) {
			ss += "," + counts[i];
		}
		outputKey.set(keyParts[0] + "," + keyParts[1] + ss);
		context.write(outputKey, null);
	}
}

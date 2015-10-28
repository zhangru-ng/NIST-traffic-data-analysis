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
		System.out.println(key.toString());
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
		double[] fvalue = new double[map.size()];
		for (Text v : values) {
			System.out.println(v.toString());
			String temp[] = v.toString().split(",");
			if (map.containsKey(temp[0])) {
				fvalue[map.get(temp[0])] += Double.parseDouble(temp[1].trim());
				counts[map.get(temp[0])]++;
			}
		}
		String[] keyParts = key.toString().split(":");
		String ss = "";
		for (int i = 0; i < counts.length; i++) {
			if (i >= 6) {
				if (counts[i] == 0)
					ss += ",0";
				else
					ss += "," + (fvalue[i] / counts[i]);
			} else
				ss += "," + counts[i];
		}
		outputKey.set(keyParts[0] + "," + keyParts[1] + "-01," + keyParts[1]
				+ "-31" + ss);
		context.write(outputKey, null);
	}
}

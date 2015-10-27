package edu.ufl.ds.nearby;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NearbyCleaningMapper extends Mapper<LongWritable, Text, Text, Text> {
	static private HashMap<String, String[]> nearbyZones = null;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (nearbyZones == null) {
			nearbyZones = new HashMap<>();
			Configuration conf = context.getConfiguration();
			String[] nearbyStr = conf.get("nearbyzones").split("#");
			for (String z : nearbyStr) {
				nearbyZones.put(z.split("\t")[0], z.split("\t")[1].split(","));
			}
		}

		String[] parts = value.toString().split("\t");
		String[] vals = parts[0].split(",");
		String zone = vals[6];
		String flow = vals[3];
		String time = vals[1].substring(0, 16);
		if (parts[1].equals("1")) {
			String[] nearbyStr = nearbyZones.get(zone);
			if (nearbyStr != null) {
    				for (String z : nearbyStr) {
    				    context.write(new Text(z + "," + time), new Text(flow));
    				}
    				context.write(new Text(zone + "," + time), new Text(flow));
			}

		}
		context.write(new Text(zone + "," + time), value);

	}
}

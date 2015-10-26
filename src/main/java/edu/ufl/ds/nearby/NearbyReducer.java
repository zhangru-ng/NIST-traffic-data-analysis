package edu.ufl.ds.nearby;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NearbyReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputVal = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	double mean = 0.0;
	double std = 0.0;
	int count = 0;
	ArrayList<Integer> flowList = new ArrayList<>();
	ArrayList<String> rowsList = new ArrayList<>();
	for (Text v : values) {
	    String[] parts = v.toString().split(",");
	    if (parts.length == 1) {
		int flow = Integer.parseInt(parts[0]);
		mean += flow;
		count++;
	    } else {
		rowsList.add(v.toString());
	    }
	}
	mean /= count;
	for (int i : flowList) {
	    std += (i - mean) * (i - mean);
	}
	std = Math.sqrt(std / count);

	for (String s : rowsList) {
	    String parts[] = s.split("\t");
	    int flow = Integer.parseInt(parts[0].toString().split(",", 4)[3]);
	    if (parts[1].equals("0")) {
		outputKey.set(s + "\t0\t" + mean + parts[2]);
	    } else if (Math.abs(flow - mean) > std) {
		outputKey.set(s + "\t0\t" + mean + "\"unsimilar for nearby zones\"");
	    } else {
		outputKey.set(s + "\t1" + flow);
	    }
	    context.write(outputKey, null);
	}
    }
}

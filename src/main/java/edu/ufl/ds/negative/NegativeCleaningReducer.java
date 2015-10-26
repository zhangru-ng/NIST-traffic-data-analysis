package edu.ufl.ds.negative;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NegativeCleaningReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputVal = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double mean = 0.0;
        double std = 0.0;
        int count = 0;
        ArrayList<Integer> flowList = new ArrayList<>();
        ArrayList<String> rowsList = new ArrayList<>();
	for (Text v : values) {
	    int flow = Integer.parseInt(v.toString().split(",", 4)[3]);
	    if (flow >= 0) {
		flowList.add(flow);
		mean += flow;
		count++;
	    }
	    rowsList.add(v.toString());
        }
	mean /= count;
	for (int i : flowList) {
	    std += (i - mean) * (i - mean) ;
	}
	std = Math.sqrt(std / count);
	for (String s : rowsList) {
	    int flow = Integer.parseInt(s.toString().split(",", 4)[3]);
	    if (flow < 0) {
		outputKey.set(s + "\t0\t" + "\"flow is negative\"");
	    } else if (Math.abs(flow - mean) > std) {
		outputKey.set(s + "\t0\t" + "\"unsimilar for same zone\"");
	    } else {
		outputKey.set(s + "\t1");
	    }
	    context.write(outputKey, null);
	}
    }
}

package edu.ufl.ds.join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputVal = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	ArrayList<String> vals = new ArrayList<String>();
	String inventory = null;
	for (Text v : values) {
	    String str = v.toString();
	    String[] parts = str.split(",", 2);
	    if (parts[0].equals("Inventory")) {
		inventory = parts[1];
	    } else {
		vals.add(parts[1]);
	    }
	}
	if (inventory != null && !vals.isEmpty()) {
	    for (String vs : vals) {
		outputVal.set(vs + "," + inventory);
		context.write(key, outputVal);
	    }
	}
    }
}

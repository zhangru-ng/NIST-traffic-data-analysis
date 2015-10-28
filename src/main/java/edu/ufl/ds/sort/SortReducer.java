package edu.ufl.ds.sort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ufl.ds.sort.SortDriver.LaneIdAndTimePair;

public class SortReducer extends Reducer<LaneIdAndTimePair, Text, Text, NullWritable> {
    private Text outputKey = new Text();
    @Override
    public void reduce(LaneIdAndTimePair key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	for (Text v : values) {
	    String[] parts = v.toString().split("\t", 2);
	    // output lane_id, measurement start and origin flow for debugging
	    // String[] vals = parts[0].split(",");
	    // outputKey.set(vals[0] + "\t" + vals[1] + "\t" + vals[3] + "\t\t| " + parts[1]);
	    if (parts[1].length() > 1) {
		outputKey.set(parts[1]);
	    } else {
		outputKey.set(parts[1] + '\t' + parts[0].split(",")[3]);
	    }
	    context.write(outputKey, null);
	}
    }
}

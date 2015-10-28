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
            // output lane_id, measurement start and origin flow for debugging
             String[] parts = v.toString().split("\t", 2);
             String[] vals = parts[0].split(",");
             outputKey.set(vals[0] + "\t" + vals[1] + "\t" + vals[3] + "\t\t| " + parts[1]);
//    	    outputKey.set(v.toString().split("\t", 2)[1]);
    	    context.write(outputKey, null);
    	}
    }
}

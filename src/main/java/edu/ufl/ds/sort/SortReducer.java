package edu.ufl.ds.sort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text outputKey = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
    	for (Text v : values) {
    	    outputKey.set(v.toString().split("\t", 2)[1]);
    	    context.write(outputKey, null);
    	}
    }
}

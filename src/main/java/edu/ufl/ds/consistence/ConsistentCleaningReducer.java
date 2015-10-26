package edu.ufl.ds.consistence;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConsistentCleaningReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputVal = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // mean and std cleaning can be done in each group
        for (Text v : values) {
            context.write(v, null);
        }
    }
}

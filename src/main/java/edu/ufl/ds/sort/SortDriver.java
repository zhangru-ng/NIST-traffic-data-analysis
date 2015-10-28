package edu.ufl.ds.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.ufl.ds.Cleaning;

public class SortDriver {

    public static class LaneIdAndTimePair implements WritableComparable<LaneIdAndTimePair> {
	    private IntWritable lane_id;
	    private LongWritable timestamp;

	    public LaneIdAndTimePair() {
	        set(new IntWritable(), new LongWritable());
	    }

	    public LaneIdAndTimePair(int lane_id, long timestamp) {
	        set(new IntWritable(lane_id), new LongWritable(timestamp));
	    }

	    public void set(IntWritable lane_id, LongWritable timestamp) {
		this.lane_id = lane_id;
	        this.timestamp = timestamp;
	    }

	    @Override
	    public int compareTo(LaneIdAndTimePair lt) {
	        int cmp = timestamp.compareTo(lt.timestamp);
	        if(0 != cmp)
	            return cmp;
	        return lane_id.compareTo(lt.lane_id);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
		lane_id.readFields(in);
		timestamp.readFields(in);
	    }
	    @Override
	    public void write(DataOutput out) throws IOException {
		lane_id.write(out);
		timestamp.write(out);
	    }

	    @Override
	    public boolean equals(Object o) {
	        if (o instanceof LaneIdAndTimePair) {
	            LaneIdAndTimePair lt = (LaneIdAndTimePair) o;
	            return lane_id.equals(lt.lane_id) && timestamp.equals(lt.timestamp);
	        }
	        return false;
	    }
	}

    public static class LaneIdAndTimeComparator extends WritableComparator {

        protected LaneIdAndTimeComparator() {
            super(LaneIdAndTimePair.class, true);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            long time1 = readLong(b1, s1+4);
            long time2 = readLong(b2, s2+4);
            int cmp = (time1 < time2) ? -1 : (time1 == time2) ? 0 : 1;
            if(0 != cmp)
                return cmp;

            int lane_id1 = readInt(b1, s1);
            int lane_id2 = readInt(b2, s2);
            return (lane_id1 < lane_id2) ? -1 : (lane_id1 == lane_id2) ? 0 : 1;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
    	    LaneIdAndTimePair pair1 = (LaneIdAndTimePair) w1;
            LaneIdAndTimePair pair2 = (LaneIdAndTimePair) w2;
            int cmp = Long.compare(pair1.timestamp.get(), pair2.timestamp.get());
            if (cmp != 0) {
                return cmp;
            } else {
                return Integer.compare(pair1.lane_id.get(), pair2.lane_id.get());
            }
        }
    }

    public static void sort(String input, String partition, String tmp)
            throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(Cleaning.outBucket), conf);

        String outputName = input.substring(7).replaceAll(".csv", "_nist7.txt").replaceAll("test", "subm");
        Path outPath = new Path(Cleaning.outBucket + "result/" + outputName);
        if (fs.exists(outPath)) {
            return;
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(SortDriver.class);

        job.setMapperClass(SortMapper.class);
        job.setOutputKeyClass(LaneIdAndTimePair.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(LaneIdAndTimeComparator.class);
        job.setReducerClass(SortReducer.class);
        job.setNumReduceTasks(1);

        Path tmpPath = new Path(tmp);

        if (fs.exists(tmpPath)) {
            fs.delete(tmpPath, true);
        }
        String inputDir = Cleaning.outBucket + "nearby/" + input.substring(7).replaceAll(".csv", "");
        FileInputFormat.addInputPath(job, new Path(inputDir));
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.waitForCompletion(true);
        FileUtil.copyMerge(fs, tmpPath, fs, outPath, true, conf, "");
    }

}

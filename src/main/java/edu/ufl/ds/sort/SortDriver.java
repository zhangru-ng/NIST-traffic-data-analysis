package edu.ufl.ds.sort;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
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

    public static class LaneIdAndTimeComparator extends WritableComparator {

        protected LaneIdAndTimeComparator() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            /*
             * parts[0]: lane_id
             * parts[1]: measurement_start
             */
            String[] pair1 = ((Text) w1).toString().split(",");
            String[] pair2 = ((Text) w2).toString().split(",");
            int lane_id1 = Integer.parseInt(pair1[0]);
            int lane_id2 = Integer.parseInt(pair2[0]);
            try {
                Date date1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(pair1[1]);
                Date date2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(pair2[1]);
                int cmp = date1.compareTo(date2);
                if (cmp != 0) {
                    return cmp;
                } else {
                    return Integer.compare(lane_id1, lane_id2);
                }
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return 1;
        }
    }

    public static void sort(Path inputPath)
            throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(Cleaning.outBucket), conf);

        String outputName = inputPath.getName().replaceAll("test", "subm") + "_nist7.txt";
        Path outPath = new Path(Cleaning.outBucket + "result/" + outputName);
        if (fs.exists(outPath)) {
            return;
        }

        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf);
        job.setJarByClass(SortDriver.class);

        job.setMapperClass(SortMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(LaneIdAndTimeComparator.class);
        job.setReducerClass(SortReducer.class);
        job.setNumReduceTasks(1);

        Path tmpPath = new Path(Cleaning.sortTmp);

        if (fs.exists(tmpPath)) {
            fs.delete(tmpPath, true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.waitForCompletion(true);

        FileUtil.copyMerge(fs, tmpPath, fs, outPath, true, conf, "");
    }

}

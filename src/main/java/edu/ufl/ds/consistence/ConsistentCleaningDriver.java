package edu.ufl.ds.consistence;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.ufl.ds.Cleaning;

public class ConsistentCleaningDriver {

    public static void consistentCleaning(Path inputPath, Path outputPath)
            throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();

        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf);
        job.setJarByClass(ConsistentCleaningDriver.class);

        job.setMapperClass(ConsistentCleaningMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(ConsistentCleaningReducer.class);

        FileSystem fs = FileSystem.get(new URI(Cleaning.outBucket), conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

}

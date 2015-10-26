package edu.ufl.ds.nearby;

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

public class NearbyCleaningDriver {

	public static void nearbyCleaning(Path inputPath, Path outputPath)
			throws ClassNotFoundException, IOException, InterruptedException,
			URISyntaxException {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(new URI(Cleaning.outBucket), conf);

		conf.set("nearbyzones", Cleaning.nearbyZones);
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf);
		job.setJarByClass(NearbyCleaningDriver.class);

		job.setMapperClass(NearbyCleaningMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(NearbyCleaningReducer.class);

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

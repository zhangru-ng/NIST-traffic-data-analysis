package edu.ufl.ds.join;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinDriver {
    /*
     * args[0]: Inventory file path
     * args[1]: Cleaning test file path
     * args[2]: Output bucket(directory) path
     */
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

	Path inputPath1 = new Path(files[0]);
	Path inputPath2 = new Path(files[1]);
	Path tempPath = new Path(files[2] + "tmp/");
	Path resultPath = new Path(files[2] + "join.out");

	FileSystem fs = FileSystem.get(new URI(files[2]), conf);
	if (fs.exists(tempPath)) {
	    fs.delete(tempPath, true);
	}

	conf.set("mapreduce.output.textoutputformat.separator", ",");

	Job job = Job.getInstance(conf);
	job.setJarByClass(JoinDriver.class);

	MultipleInputs.addInputPath(job, inputPath1,  TextInputFormat.class, InventoryMapper.class);
	MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, CleanTestMapper.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setReducerClass(JoinReducer.class);

	job.setOutputFormatClass(TextOutputFormat.class);
	FileOutputFormat.setOutputPath(job, tempPath);

	job.waitForCompletion(true);

	FileUtil.copyMerge(fs, tempPath, fs, resultPath, false, conf, "");
    }
}

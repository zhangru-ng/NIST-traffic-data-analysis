package edu.ufl.ds.consistence;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortDriver {
    private static String outBucket = "";
    private static String tmp = "";
    private static String result = "";
    private static String resultPrefix = "";

    public static class LaneIdAndTimeComparator extends WritableComparator {

	protected LaneIdAndTimeComparator() {
	    super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
	    /*
	     * parts[0]: lane_id
	     * parts[1]: measurement start
	     */
	    String[] parts1 = ((Text) w1).toString().split(",");
	    String[] parts2 = ((Text) w2).toString().split(",");
	    int lane_id1 = Integer.parseInt(parts1[0]);
	    int lane_id2 = Integer.parseInt(parts2[0]);
	    try {
		Date date1 = new SimpleDateFormat("HH:mm:ss").parse(parts1[1].split(" ")[2]);
		Date date2 = new SimpleDateFormat("HH:mm:ss").parse(parts2[1].split(" ")[2]);
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

    public static void sortDriver(Path inputPath)
	    throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
	Configuration conf = new Configuration();

	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
	job.setJarByClass(SortDriver.class);

	job.setMapperClass(SortMapper.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setReducerClass(SortReducer.class);

	String cleanTestName = inputPath.getName();
	Path tmpPath = new Path(tmp + cleanTestName);

	FileSystem fs = FileSystem.get(new URI(outBucket), conf);
	if (fs.exists(tmpPath)) {
	    fs.delete(tmpPath, true);
	}
	Path resultPath = new Path(result + resultPrefix + cleanTestName);

	FileInputFormat.addInputPath(job, inputPath);
	job.setInputFormatClass(TextInputFormat.class);

	job.setOutputFormatClass(TextOutputFormat.class);
	FileOutputFormat.setOutputPath(job, tmpPath);

	job.waitForCompletion(true);
	FileUtil.copyMerge(fs, tmpPath, fs, resultPath, true, conf, "");
    }
    /*
     * args[0]: Cleaning test directory path
     * args[1]: Output bucket(directory) path
     */
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] paths = new GenericOptionsParser(conf, args).getRemainingArgs();

	Path inputPath = new Path(paths[0]);
	outBucket = paths[1];
	tmp = outBucket + "tmp/";
	result = outBucket + "result/";

	FileSystem fs = FileSystem.get(new URI(paths[1]), conf);
	// run on all cleaning_test file, run each file separately to preserve file name in result
	RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(inputPath, false);
	while (fileIterator.hasNext()) {
	    LocatedFileStatus stat = fileIterator.next();
	    SortDriver.sortDriver(stat.getPath());
	}
    }
}

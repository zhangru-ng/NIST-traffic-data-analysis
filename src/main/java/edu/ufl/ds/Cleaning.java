package edu.ufl.ds;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.ufl.ds.consistence.ConsistentCleaningDriver;
import edu.ufl.ds.nearby.NearbyCleaningDriver;
import edu.ufl.ds.negative.NegativeCleaningDriver;
import edu.ufl.ds.sort.SortDriver;

public class Cleaning {
    static public String outBucket = "";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] paths = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path inputPath = new Path(paths[0]);
        outBucket = paths[1];

        FileSystem fs = FileSystem.get(new URI(paths[1]), conf);
        // run on all cleaning_test file, run each file separately to preserve file name in result
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(inputPath, false);
        Path negativeOut = new Path(outBucket + "negative/");
        Path consistentOut = new Path(outBucket + "consistent/");
        Path nearbyOut = new Path(outBucket + "nearby/");
        while (fileIterator.hasNext()) {
            LocatedFileStatus stat = fileIterator.next();
            NegativeCleaningDriver.negativeCleaning(stat.getPath(), negativeOut);
            ConsistentCleaningDriver.consistentCleaning(negativeOut, consistentOut);
            NearbyCleaningDriver.nearbyCleaning(consistentOut, nearbyOut);
            SortDriver.sort(nearbyOut, stat.getPath().getName());
        }
    }
}

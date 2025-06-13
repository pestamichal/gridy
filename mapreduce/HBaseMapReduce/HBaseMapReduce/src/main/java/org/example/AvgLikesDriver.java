package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class AvgLikesDriver {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zoo");
        Job job = Job.getInstance(config, "Avg Likes Per Platform");
        job.setJarByClass(AvgLikesDriver.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Platform"));
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Likes"));

        TableMapReduceUtil.initTableMapperJob(
                "social_media",
                scan,
                AvgLikesMapper.class,
                Text.class,
                LongWritable.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                "platform_likes_avg",
                AvgLikesReducer.class,
                job
        );

        job.setNumReduceTasks(1);

        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis(); // End timing
        long duration = endTime - startTime;

        System.out.println("🕒 Job completed in " + duration + " ms");

        System.exit(success ? 0 : 1);
    }
}
package org.example;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class AvgLikesReducer extends TableReducer<Text, LongWritable, Text> {
    private static final byte[] CF = Bytes.toBytes("cf");

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        long count = 0;

        for (LongWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count > 0) {
            double avg = (double) sum / count;
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(CF, Bytes.toBytes("avg_likes"), Bytes.toBytes(String.valueOf(avg)));
            context.write(null, put);
        }
    }
}
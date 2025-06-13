package org.example;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class AvgLikesMapper extends TableMapper<Text, LongWritable> {
    private static final byte[] CF = Bytes.toBytes("cf");
    private static final byte[] PLATFORM = Bytes.toBytes("Platform");
    private static final byte[] LIKES = Bytes.toBytes("Likes");

    @Override
    protected void map(ImmutableBytesWritable rowKey, Result result, Context context)
            throws IOException, InterruptedException {

        byte[] platformBytes = result.getValue(CF, PLATFORM);
        byte[] likesBytes = result.getValue(CF, LIKES);

        if (platformBytes != null && likesBytes != null) {
            String platform = Bytes.toString(platformBytes).trim();
            try {
                long likes = Long.parseLong(Bytes.toString(likesBytes).trim());
                context.write(new Text(platform), new LongWritable(likes));
            } catch (NumberFormatException e) {
                // Skip rows with invalid number
            }
        }
    }
}
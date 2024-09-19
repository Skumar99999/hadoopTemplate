import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer class to get counts of flights for each origin airport.
 */
public class ReducerOne extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * Reduces flight counts per airport.
     *
     * @param key     Origin airport.
     * @param values  Iterable list of flight counts.
     * @param context Output where final count per airport is written.
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
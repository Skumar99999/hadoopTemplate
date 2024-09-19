import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer class to compute delay ratio for each airline.
 */
public class ReducerTwo extends Reducer<Text, DelayCountWritable, Text, DoubleWritable> {
    /**
     * Reduces total delay and flight count for each airline and computes delay ratio.
     *
     * @param key     Airline.
     * @param values  Iterable list of total delays and flight counts.
     * @param context Output where final delay ratio per airline is written.
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<DelayCountWritable> values, Context context) throws IOException, InterruptedException {
        double totalDelay = 0;
        int totalFlights = 0;
        for (DelayCountWritable val : values) {
            totalDelay += val.getTotalDelay().get();
            totalFlights += val.getFlightCount().get();
        }
        double delayRatio = totalFlights == 0 ? 0 : totalDelay / totalFlights;
        context.write(key, new DoubleWritable(delayRatio));
    }
}
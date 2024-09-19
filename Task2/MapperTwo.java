import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class to map airline with total departure delay and flight count.
 */
public class MapperTwo extends Mapper<Object, Text, Text, DelayCountWritable> {
    private Text airline = new Text();

    /**
     * Maps each airline to total departure delay and count of flights.
     *
     * @param key     Input key (ignored).
     * @param value   Line of input data.
     * @param context Output where mapped airline and delay information is written.
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 11) {
            airline.set(fields[4]);
            try {
                double delay = Double.parseDouble(fields[11]);
                context.write(airline, new DelayCountWritable(delay, 1));
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }
    }
}
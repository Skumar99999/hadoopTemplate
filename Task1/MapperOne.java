import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class to map origin airport with value 1 for each flight.
 */
public class MapperOne extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text airport = new Text();

    /**
     * Maps origin airport with value of 1 for each flight record.
     *
     * @param key     Input key (ignored).
     * @param value   Line of input data.
     * @param context Output where mapped airport and count is written.
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 7) {
            airport.set(fields[7]);
            context.write(airport, one);
        }
    }
}
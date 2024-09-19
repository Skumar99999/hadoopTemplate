import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom writable class to hold total delay and flight count for each airline.
 */
public class DelayCountWritable implements Writable {
    private DoubleWritable totalDelay;
    private IntWritable flightCount;

    public DelayCountWritable() {
        this.totalDelay = new DoubleWritable(0);
        this.flightCount = new IntWritable(0);
    }

    public DelayCountWritable(double totalDelay, int flightCount) {
        this.totalDelay = new DoubleWritable(totalDelay);
        this.flightCount = new IntWritable(flightCount);
    }

    /**
     * Writes total delay and flight count to output stream.
     *
     * @param out Data output stream.
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        totalDelay.write(out);
        flightCount.write(out);
    }

    /**
     * Reads total delay and flight count from input stream.
     *
     * @param in Data input stream.
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        totalDelay.readFields(in);
        flightCount.readFields(in);
    }

    public DoubleWritable getTotalDelay() {
        return totalDelay;
    }

    public IntWritable getFlightCount() {
        return flightCount;
    }
}
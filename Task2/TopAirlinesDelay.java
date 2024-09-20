import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver class for to calculate largest departure delay ratio by airline.
 */
public class TopAirlinesDelay {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "airline delay ratio");
        job.setJarByClass(TopAirlinesDelay.class);
        job.setMapperClass(MapperTwo.class);
        job.setCombinerClass(ReducerTwo.class);
        job.setReducerClass(ReducerTwo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DelayCountWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
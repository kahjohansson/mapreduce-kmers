import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmers {

    public static class KmersMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final int string_length = 9;

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String value_sequence = value.toString();
            for (int i = 0; i <= value_sequence.length() - string_length; i++) {
                word.set(value_sequence.substring(i, i + string_length));
                context.write(word, one);
            }
        }
    }

    public static class KmersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job_kmers = Job.getInstance(conf, "kmers_step");
        job_kmers.setJarByClass(Kmers.class);
        job_kmers.setMapperClass(KmersMapper.class);
        job_kmers.setCombinerClass(KmersReducer.class);
        job_kmers.setReducerClass(KmersReducer.class);
        job_kmers.setOutputKeyClass(Text.class);
        job_kmers.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job_kmers, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_kmers, new Path(args[1]));

        System.exit(job_kmers.waitForCompletion(true) ? 0 : 1);
    }

}

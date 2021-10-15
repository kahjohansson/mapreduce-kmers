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

public class Sort {

    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        
        public void map(Object key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            IntWritable count = new IntWritable(Integer.parseInt(data[1]) * -1);
            Text kmer_string = new Text(data[0]);
            context.write(count, kmer_string);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {
            for (Text value : values) {
                key.set(key.get() * -1);
                context.write(value, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job_sort = Job.getInstance(conf, "sort_step");
        job_sort.setJarByClass(Sort.class);
        job_sort.setMapperClass(SortMapper.class);
        job_sort.setReducerClass(SortReducer.class);
        job_sort.setOutputKeyClass(IntWritable.class);
        job_sort.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job_sort, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_sort, new Path(args[1]));

        System.exit(job_sort.waitForCompletion(true) ? 0 : 1);
    }

}

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Java Task 1: Rating histogram.
 *
 * Input : reviews_100k.csv
 *   (user_id,book_id,review_id,rating,review_text,date_added,date_updated,
 *    read_at,started_at,n_votes,n_comments)
 *
 * Output: rating <TAB> number_of_reviews
 *
 * A word-count-style job: introduces InputFormat handling, header skipping,
 * a Combiner, and CSV-safe field extraction (review_text itself may contain
 * commas and quotes, so the naive value.toString().split(",") would break).
 */
public class RatingHistogram {

    public static class RatingMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text ratingKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("user_id,book_id,review_id,rating")) {
                return; // header row
            }
            String[] fields = CsvUtils.parseCsvLine(line);
            if (fields.length < 4) {
                return;
            }
            String rating = fields[3].trim();
            if (rating.isEmpty()) {
                return;
            }
            ratingKey.set(rating);
            context.write(ratingKey, ONE);
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RatingHistogram <input reviews_100k.csv on HDFS> <output dir>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rating Histogram");
        job.setJarByClass(RatingHistogram.class);
        job.setMapperClass(RatingMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

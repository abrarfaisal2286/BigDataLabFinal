import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Java Task 2: Average book rating by publication year.
 *
 * Input : books.csv
 *   (book_id,title,authors,average_rating,ratings_count,publication_year,
 *    language_code,isbn)
 *
 * Output: publication_year <TAB> average_rating <TAB> number_of_books
 *
 * Demonstrates why you cannot just average partial averages: both the
 * Combiner and the Reducer emit "sum:count" pairs, and only the final
 * Reducer divides sum by count. Rows with a blank/invalid year or rating
 * are dropped (about 25% of books.csv has no publication_year).
 */
public class AvgRatingByYear {

    public static class YearMapper extends Mapper<Object, Text, Text, Text> {
        private final Text yearKey = new Text();
        private final Text sumCount = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("book_id,title,authors")) {
                return; // header row
            }
            String[] fields = CsvUtils.parseCsvLine(line);
            if (fields.length < 6) {
                return;
            }

            String year = fields[5].trim();
            if (year.isEmpty() || !year.matches("\\d{4}")) {
                return;
            }

            double rating;
            try {
                rating = Double.parseDouble(fields[3].trim());
            } catch (NumberFormatException e) {
                return;
            }

            yearKey.set(year);
            sumCount.set(rating + ":1");
            context.write(yearKey, sumCount);
        }
    }

    /** Pre-aggregates sum/count pairs on the mapper side. */
    public static class SumCountCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0L;
            for (Text v : values) {
                String[] parts = v.toString().split(":");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }
            out.set(sum + ":" + count);
            context.write(key, out);
        }
    }

    /** Final aggregation: turns the accumulated sum/count into an average. */
    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0L;
            for (Text v : values) {
                String[] parts = v.toString().split(":");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }
            double avg = (count == 0) ? 0.0 : sum / count;
            out.set(String.format("%.3f\t%d", avg, count));
            context.write(key, out);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AvgRatingByYear <input books.csv on HDFS> <output dir>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating By Year");
        job.setJarByClass(AvgRatingByYear.class);
        job.setMapperClass(YearMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

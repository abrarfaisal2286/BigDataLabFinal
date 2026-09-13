import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Java Task 3: Reduce-side join of books.csv and reviews_100k.csv on book_id.
 *
 * For every book that has at least one review, outputs:
 *   book_id <TAB> title <TAB> review_count <TAB> average_review_rating
 *
 * Two Mappers read the two different files and tag each value so the single
 * Reducer can tell which side it came from once both are shuffled together
 * on the same book_id key: "B|<title>" for books.csv, "R|<rating>" for
 * reviews_100k.csv. This is the classic reduce-side join pattern, done here
 * with MultipleInputs instead of a Pig JOIN, to show the same logic at the
 * MapReduce level.
 */
public class BookReviewJoin {

    public static class BooksMapper extends Mapper<Object, Text, Text, Text> {
        private final Text bookIdKey = new Text();
        private final Text tagged = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("book_id,title,authors")) {
                return; // header row
            }
            String[] fields = CsvUtils.parseCsvLine(line);
            if (fields.length < 2) {
                return;
            }
            String bookId = fields[0].trim();
            String title = fields[1].trim();
            if (bookId.isEmpty()) {
                return;
            }
            bookIdKey.set(bookId);
            tagged.set("B|" + title);
            context.write(bookIdKey, tagged);
        }
    }

    public static class ReviewsMapper extends Mapper<Object, Text, Text, Text> {
        private final Text bookIdKey = new Text();
        private final Text tagged = new Text();

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
            String bookId = fields[1].trim();
            String rating = fields[3].trim();
            if (bookId.isEmpty() || rating.isEmpty()) {
                return;
            }
            bookIdKey.set(bookId);
            tagged.set("R|" + rating);
            context.write(bookIdKey, tagged);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text bookId, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String title = null;
            double sum = 0.0;
            long count = 0L;

            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("B|")) {
                    title = s.substring(2);
                } else if (s.startsWith("R|")) {
                    try {
                        sum += Double.parseDouble(s.substring(2));
                        count++;
                    } catch (NumberFormatException ignored) {
                        // skip malformed rating value
                    }
                }
            }

            if (count == 0) {
                return; // only emit books that actually have reviews
            }
            double avg = sum / count;
            out.set((title == null ? "UNKNOWN_TITLE" : title)
                    + "\t" + count + "\t" + String.format("%.3f", avg));
            context.write(bookId, out);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: BookReviewJoin <books.csv on HDFS> <reviews_100k.csv on HDFS> <output dir>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Book Review Reduce-Side Join");
        job.setJarByClass(BookReviewJoin.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, BooksMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReviewsMapper.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

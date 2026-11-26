package nasa.etl.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class MapReduceJobs {
    private static final String MONTHS = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec";
    private static final Pattern LINE = Pattern.compile(
            "(?d)^(\\S+) \\S+ \\S+ \\[((0[1-9]|[12]\\d|3[01])/(" + MONTHS
                    + ")/(\\d{4}):([01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d [+-]\\d{4})\\] \"(.*)\" (\\d{3}) (\\d+|-)$");
    private static final Pattern REQUEST = Pattern.compile(
            "(?d)^(\\S+)\\s+(.+?)(?:\\s+(HTTP/\\S+))?$");
    private static final String[] MONTH_NAMES = {
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    };
    private static final String[] MONTH_NUMBERS = {
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"
    };

    private MapReduceJobs() {}

    private static String escape(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '\\') {
                escaped.append("\\\\");
            } else if (ch < 0x20 || ch > 0x7e) {
                escaped.append(String.format("\\x%02X", (int) ch));
            } else {
                escaped.append(ch);
            }
        }
        return escaped.toString();
    }

    private static String unescape(String value) {
        StringBuilder decoded = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch != '\\') {
                decoded.append(ch);
            } else if (i + 1 < value.length() && value.charAt(i + 1) == '\\') {
                decoded.append('\\');
                i++;
            } else if (i + 3 < value.length() && value.charAt(i + 1) == 'x') {
                decoded.append((char) Integer.parseInt(value.substring(i + 2, i + 4), 16));
                i += 3;
            } else {
                throw new IllegalArgumentException("Invalid escaped value: " + value);
            }
        }
        return decoded.toString();
    }

    private static int monthIndex(String month) {
        for (int i = 0; i < MONTH_NAMES.length; i++) {
            if (MONTH_NAMES[i].equals(month)) return i;
        }
        throw new IllegalArgumentException("Invalid month: " + month);
    }

    private static String[] fields(Text text) {
        return text.toString().split("\t", -1);
    }

    private static Job newJob(Configuration conf, String name, Class<? extends Mapper> mapper,
                              Class<? extends Reducer> reducer,
                              Class<?> mapKey, Class<?> mapValue, Class<?> outKey, Class<?> outValue,
                              String input, String output, int reducers) throws IOException {
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(MapReduceJobs.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setMapOutputKeyClass(mapKey);
        job.setMapOutputValueClass(mapValue);
        job.setOutputKeyClass(outKey);
        job.setOutputValueClass(outValue);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }

    public static class ParseMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private long records;
        private long parsed;
        private long malformed;

        @Override
        protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            records++;
            String line = new String(text.getBytes(), 0, text.getLength(), StandardCharsets.ISO_8859_1);
            if (line.endsWith("\r")) {
                line = line.substring(0, line.length() - 1);
            }
            Matcher lineMatch = LINE.matcher(line);
            if (!lineMatch.matches()) {
                malformed++;
                return;
            }

            String host = lineMatch.group(1);
            String timestamp = lineMatch.group(2);
            String day = lineMatch.group(3);
            String month = lineMatch.group(4);
            String year = lineMatch.group(5);
            String hour = lineMatch.group(6);
            String request = lineMatch.group(7).replaceAll("(?d)^ +| +$", "");
            String status = lineMatch.group(8);
            String bytes = lineMatch.group(9);

            Matcher requestMatch = REQUEST.matcher(request);
            String method;
            String path;
            String protocol;
            if (requestMatch.matches()) {
                method = requestMatch.group(1);
                path = requestMatch.group(2);
                protocol = requestMatch.group(3) == null ? "" : requestMatch.group(3);
            } else {
                method = request;
                path = "";
                protocol = "";
            }
            String date = year + "-" + MONTH_NUMBERS[monthIndex(month)] + "-" + day;
            String record = String.join("\t", escape(host), escape(timestamp), date, hour,
                    escape(method), escape(path), escape(protocol), status,
                    bytes.equals("-") ? "0" : bytes);
            context.write(NullWritable.get(), new Text(record));
            parsed++;
        }

        @Override
        protected void cleanup(Context context) {
            context.getCounter("ETL", "etl_records").increment(records);
            context.getCounter("ETL", "etl_parsed").increment(parsed);
            context.getCounter("ETL", "etl_malformed").increment(malformed);
        }
    }

    public static class PairWritable implements Writable {
        private long count;
        private long bytes;

        public PairWritable() {}

        PairWritable(long count, long bytes) {
            this.count = count;
            this.bytes = bytes;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(count);
            out.writeLong(bytes);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count = in.readLong();
            bytes = in.readLong();
        }
    }

    public static class Q1Mapper extends Mapper<LongWritable, Text, Text, PairWritable> {
        @Override
        protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            String[] row = fields(text);
            if (row.length != 9) throw new IOException("Invalid parsed record: " + text);
            context.write(new Text(row[2] + "\t" + row[7]), new PairWritable(1, Long.parseLong(row[8])));
        }
    }

    public static class Q1Combiner extends Reducer<Text, PairWritable, Text, PairWritable> {
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            long bytes = 0;
            for (PairWritable value : values) {
                count += value.count;
                bytes += value.bytes;
            }
            context.write(key, new PairWritable(count, bytes));
        }
    }

    public static class Q1Reducer extends Reducer<Text, PairWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<PairWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            long bytes = 0;
            for (PairWritable value : values) {
                count += value.count;
                bytes += value.bytes;
            }
            context.write(key, new Text(count + "\t" + bytes));
        }
    }

    public static class Q2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            String[] row = fields(text);
            if (row.length != 9) throw new IOException("Invalid parsed record: " + text);
            context.write(new Text(row[5]), new Text(row[0] + "\t" + row[8]));
        }
    }

    public static class Q2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text path, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            long bytes = 0;
            Set<String> hosts = new HashSet<>();
            for (Text value : values) {
                String[] pair = fields(value);
                if (pair.length != 2) throw new IOException("Invalid Q2 mapper value: " + value);
                count++;
                bytes += Long.parseLong(pair[1]);
                hosts.add(pair[0]);
            }
            context.write(path, new Text(count + "\t" + bytes + "\t" + hosts.size()));
        }
    }

    public static class RankKey implements WritableComparable<RankKey> {
        private long count;
        private String path = "";

        public RankKey() {}

        RankKey(long count, String path) {
            this.count = count;
            this.path = path;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(count);
            Text.writeString(out, path);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count = in.readLong();
            path = Text.readString(in);
        }

        @Override
        public int compareTo(RankKey other) {
            int countOrder = Long.compare(other.count, count);
            return countOrder != 0 ? countOrder : path.compareTo(other.path);
        }
    }

    public static class Q2TopMapper extends Mapper<LongWritable, Text, RankKey, Text> {
        @Override
        protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            String[] row = fields(text);
            if (row.length != 4) throw new IOException("Invalid Q2 aggregate: " + text);
            String path = unescape(row[0]);
            context.write(new RankKey(Long.parseLong(row[1]), path), new Text(text));
        }
    }

    public static class Q2TopReducer extends Reducer<RankKey, Text, NullWritable, Text> {
        private int emitted;

        @Override
        protected void reduce(RankKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (emitted < 20) {
                for (Text value : values) {
                    if (emitted == 20) break;
                    context.write(NullWritable.get(), value);
                    emitted++;
                }
            }
        }
    }

    public static class Q3Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            String[] row = fields(text);
            if (row.length != 9) throw new IOException("Invalid parsed record: " + text);
            int status = Integer.parseInt(row[7]);
            boolean error = status >= 400 && status <= 599;
            context.write(new Text(row[2] + "\t" + row[3]),
                    new Text((error ? "1" : "0") + "\t" + (error ? row[0] : "")));
        }
    }

    public static class Q3Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long errors = 0;
            long total = 0;
            Set<String> hosts = new HashSet<>();
            for (Text value : values) {
                String[] pair = fields(value);
                if (pair.length != 2) throw new IOException("Invalid Q3 mapper value: " + value);
                total++;
                if (pair[0].equals("1")) {
                    errors++;
                    hosts.add(pair[1]);
                }
            }
            context.write(key, new Text(errors + "\t" + total + "\t"
                    + Double.toString((double) errors / total) + "\t" + hosts.size()));
        }
    }

    private static boolean runJob(Job job) throws Exception {
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("Expected parse, q1, q2-aggregate, q2-top20, q3, or counters");
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", "\n");
        String action = args[0];
        if (args.length != 3) throw new IllegalArgumentException("Expected action, input and output paths");
        String input = args[1];
        String output = args[2];
        Job job;
        switch (action) {
            case "parse":
                job = newJob(conf, "etl-mapreduce-parse", ParseMapper.class, Reducer.class,
                        LongWritable.class, Text.class, NullWritable.class, Text.class, input, output, 0);
                if (!runJob(job)) throw new IOException("Parse job failed");
                org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
                System.out.println("ETL_COUNTERS " + counters.findCounter("ETL", "etl_records").getValue() + " "
                        + counters.findCounter("ETL", "etl_parsed").getValue() + " "
                        + counters.findCounter("ETL", "etl_malformed").getValue());
                return;
            case "q1":
                job = newJob(conf, "etl-mapreduce-q1", Q1Mapper.class, Q1Reducer.class,
                        Text.class, PairWritable.class, Text.class, Text.class, input, output, 1);
                job.setCombinerClass(Q1Combiner.class);
                break;
            case "q2-aggregate":
                job = newJob(conf, "etl-mapreduce-q2-aggregate", Q2Mapper.class, Q2Reducer.class,
                        Text.class, Text.class, Text.class, Text.class, input, output, 1);
                break;
            case "q2-top20":
                job = newJob(conf, "etl-mapreduce-q2-top20", Q2TopMapper.class, Q2TopReducer.class,
                        RankKey.class, Text.class, NullWritable.class, Text.class, input, output, 1);
                break;
            case "q3":
                job = newJob(conf, "etl-mapreduce-q3", Q3Mapper.class, Q3Reducer.class,
                        Text.class, Text.class, Text.class, Text.class, input, output, 1);
                break;
            default:
                throw new IllegalArgumentException("Unknown action: " + action);
        }
        if (!runJob(job)) throw new IOException(action + " job failed");
    }
}

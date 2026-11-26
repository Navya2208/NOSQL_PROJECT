package nasa.etl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Loads a text file as one chararray per line, decoded as Latin-1 (SPEC.md section 1).
 *
 * Pig's own TextLoader decodes text as UTF-8, which would change the 18 NASA lines that
 * contain non-ASCII bytes. This loader only changes how bytes become characters; it does
 * no parsing, trimming or filtering. Lines are split on "\n" only, so a trailing "\r" stays
 * in the line for parse.pig to remove (SPEC.md section 1).
 */
public class Latin1TextLoader extends LoadFunc {
    private RecordReader<?, ?> reader;
    private final TupleFactory tuples = TupleFactory.getInstance();

    @Override
    public void setLocation(String location, Job job) throws IOException {
        job.getConfiguration().set("textinputformat.record.delimiter", "\n");
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public InputFormat getInputFormat() {
        return new TextInputFormat();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }
            Text value = (Text) reader.getCurrentValue();
            String line = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.ISO_8859_1);
            return tuples.newTuple(line);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}


package nasa.etl;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Latin1LineSerDe extends AbstractSerDe {

    private StructObjectInspector inspector;

    @Override
    public void initialize(Configuration conf, Properties tbl,
                       Properties partitionProperties)
        throws SerDeException {

    super.initialize(conf, tbl, partitionProperties);

    inspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            Collections.singletonList("line"),
            Collections.singletonList(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
            )
        );
}
    @Override
    public Object deserialize(Writable blob) throws SerDeException {

        Text value = (Text) blob;

        String line = new String(
            value.getBytes(),
            0,
            value.getLength(),
            StandardCharsets.ISO_8859_1
        );

        return Collections.singletonList(line);
    }

    @Override
    public ObjectInspector getObjectInspector() {
        return inspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector inspector)
            throws SerDeException {
        throw new UnsupportedOperationException("Serialization is not supported");
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}

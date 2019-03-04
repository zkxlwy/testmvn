import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;


import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class MyTest {
    @Test
    public void testSchemaResolution() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/StringPair.avsc"));
        Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/NewStringPair.avsc"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
        GenericRecord datum = new GenericData.Record(schema); // no description

        datum.put("left", "L");
        datum.put("right", "R");
        writer.write(datum, encoder);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, new File("user.data"));
        dataFileWriter.append(datum);
        dataFileWriter.close();
        encoder.flush();

// vv AvroSchemaResolution
        DatumReader<GenericRecord> reader =
                /*[*/new GenericDatumReader<GenericRecord>(schema, newSchema);/*]*/
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),
                null);
        GenericRecord result = reader.read(null, decoder);
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
        /*[*/assertThat(result.get("description").toString(), is(""));/*]*/
        System.out.println(out.size());



// ^^ AvroSchemaResolution
    }
}
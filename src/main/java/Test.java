import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class Test {

    private Schema.Parser parser; //解析avsc文件
    private Schema schema;
    private Schema newSchema;//avsc文件
    private GenericRecord datum; //avsc文件中的结构所组成的类，本例User

    public Test() throws IOException {
        parser = new Schema.Parser();
        schema = parser.parse(getClass().getResourceAsStream("/user.avsc"));
        newSchema = parser.parse(getClass().getResourceAsStream("/newUser.avsc"));
        datum = new GenericData.Record(schema);
    }

    public void createRecord() {
        datum.put("name", "tom");
        datum.put("favorite_number", 17);
        datum.put("favorite_color", "green");
    }

    public void output() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        System.out.println(out.size());
        out.close();
    }

    public void newUser() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema,newSchema);
        Decoder encoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        reader.read(null,encoder);

        System.out.println(out.size());
        out.close();
    }
    public void outputToFile() throws IOException {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, new File("user.data"));
        dataFileWriter.append(datum);
        dataFileWriter.close();
    }
    public static void main(String[] args) throws IOException {
        Test t = new Test();
        t.createRecord();
        t.outputToFile();
    }
}
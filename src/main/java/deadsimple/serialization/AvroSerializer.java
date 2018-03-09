package deadsimple.serialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {
  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] serialize(String topic, T object) {
    try {
      byte[] result = null;

      if (object != null) {
        Schema schema = object.getSchema();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder =
            EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.write(object, encoder);

        encoder.flush();
        byteArrayOutputStream.close();

        result = byteArrayOutputStream.toByteArray();
      }
      return result;

    } catch (IOException ex) {
      throw new SerializationException("Error serializing avro message", ex);
    }
  }
}

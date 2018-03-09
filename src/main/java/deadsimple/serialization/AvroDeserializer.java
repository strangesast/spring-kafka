package deadsimple.serialization;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
  protected final Class<T> type;

  private final DecoderFactory decoderFactory = DecoderFactory.get();

  public AvroDeserializer(Class<T> type) {
    this.type = type;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @SuppressWarnings("unchecked")
  @Override
  public T deserialize(String topic, byte[] data) {
    try {
      T result = null;

      if (data != null) {
        DatumReader<GenericRecord> datumReader =
            new SpecificDatumReader<>(type.newInstance().getSchema());
        Decoder decoder = decoderFactory.binaryDecoder(data, null);

        result = (T) datumReader.read(null, decoder);
      }

      return result;

    } catch (Exception ex) {
      throw new SerializationException("Error deserializing avro message", ex);
    }
  }
}

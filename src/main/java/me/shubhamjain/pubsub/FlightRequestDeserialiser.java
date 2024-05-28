package me.shubhamjain.pubsub;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import me.shubhamjain.model.FlightRequestMetric;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FlightRequestDeserialiser implements PubSubDeserializationSchema<FlightRequestMetric> {
    private static final Logger LOG = LoggerFactory.getLogger(FlightRequestMetric.class);

    @Override
    public FlightRequestMetric deserialize(PubsubMessage message) throws IOException {
        SpecificDatumReader<FlightRequestMetric> reader = new SpecificDatumReader<>(FlightRequestMetric.getClassSchema());
        ByteString data = message.getData();
        // Get the schema encoding type.
        String encoding = message.getAttributesMap().get("googclient_schemaencoding");
        // Send the message data to a byte[] input stream.
        InputStream inputStream = new ByteArrayInputStream(data.toByteArray());
        Decoder decoder = null;
        // Prepare an appropriate decoder for the message data in the input stream
        // based on the schema encoding type.
        try {
            switch (encoding) {
                case "BINARY":
                    decoder = DecoderFactory.get().directBinaryDecoder(inputStream, /*reuse=*/ null);
                    break;
                case "JSON":
                    decoder = DecoderFactory.get().jsonDecoder(FlightRequestMetric.getClassSchema(), inputStream);
                    break;
                default:
            }
        } catch (IOException e) {
            LOG.error("Error occurred in deserializing avro message {} ", e);
        }
        return reader.read(null, decoder);
    }

    @Override
    public boolean isEndOfStream(FlightRequestMetric flightRequestMetric) {
        return false;
    }

    @Override
    public TypeInformation<FlightRequestMetric> getProducedType() {
        return TypeInformation.of(FlightRequestMetric.class);
    }

}
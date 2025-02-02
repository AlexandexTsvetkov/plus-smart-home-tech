package ru.practicum.util;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class GeneralAvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private BinaryEncoder encoder;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Конфигурация (при необходимости)
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] result = null;
            encoder = encoderFactory.binaryEncoder(out, encoder);

            if (data != null) {
                DatumWriter<T> writer = new SpecificDatumWriter<>(data.getSchema());
                writer.write(data, encoder);
                encoder.flush();
                result = out.toByteArray();
            }
            return result;
        } catch (IOException ex) {
            throw new SerializationException("Ошибка сериализации данных для топика [" + topic + "]", ex);
        }
    }
}
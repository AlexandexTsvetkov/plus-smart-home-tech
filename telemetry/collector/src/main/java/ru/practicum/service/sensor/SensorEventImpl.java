package ru.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.practicum.dto.sensor.SensorEvent;
import ru.practicum.mapper.SensorMapper;
import ru.practicum.util.KafkaProperties;

@Service
@Slf4j
@RequiredArgsConstructor
public class SensorEventImpl implements SensorService {

    private final Producer<String, SpecificRecordBase> producer;

    private final KafkaProperties kafkaProperties;

    @Override
    public void sendSensorEvent(SensorEvent sensorEvent) {
        producer.send(new ProducerRecord<>(kafkaProperties.getSensorEventTopic(),
                null,
                sensorEvent.getTimestamp().toEpochMilli(),
                sensorEvent.getId(),
                SensorMapper.mapToSensorEventAvro(sensorEvent)));
    }
}


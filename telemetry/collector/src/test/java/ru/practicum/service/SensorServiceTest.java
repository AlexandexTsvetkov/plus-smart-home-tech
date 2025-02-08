package ru.practicum.service;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import ru.practicum.dto.sensor.ClimateSensorEvent;
import ru.practicum.service.sensor.SensorService;
import ru.practicum.util.KafkaProperties;

import java.time.Instant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
class SensorServiceTest {

    @Autowired
    private SensorService sensorService;

    @Autowired
    private KafkaProperties kafkaProperties;

    @MockBean
    private Producer<String, SpecificRecordBase> producer;

    @Test
    public void sensorServiceTest() {

        ClimateSensorEvent climateSensorEvent = new ClimateSensorEvent();
        climateSensorEvent.setId("1");
        climateSensorEvent.setHumidity(10);
        climateSensorEvent.setCo2Level(5);
        climateSensorEvent.setTimestamp(Instant.now());
        climateSensorEvent.setHubId("2");
        climateSensorEvent.setTemperatureC(4);

        sensorService.sendSensorEvent(climateSensorEvent);

        ArgumentCaptor<ProducerRecord<String, SpecificRecordBase>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer, times(1)).send(captor.capture());
        ProducerRecord<String, SpecificRecordBase> producerRecord = captor.getValue();
        assertThat(producerRecord).isNotNull();
        assertThat(producerRecord.topic()).isEqualTo(kafkaProperties.getSensorEventTopic());
        assertThat(producerRecord.key()).isEqualTo(climateSensorEvent.getId());
        assertThat(producerRecord.value()).isNotNull();
    }
}
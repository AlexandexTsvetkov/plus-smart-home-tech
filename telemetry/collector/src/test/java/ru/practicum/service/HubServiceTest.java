package ru.practicum.service;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import ru.practicum.dto.hub.device.DeviceAddedEvent;
import ru.practicum.dto.hub.device.DeviceType;
import ru.practicum.service.hub.HubServise;
import ru.practicum.util.KafkaProperties;

import java.time.Instant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
class HubServiceTest {

    @Autowired
    private HubServise hubServise;

    @Autowired
    private KafkaProperties kafkaProperties;

    @MockBean
    private Producer<String, SpecificRecordBase> producer;

    @Test
    public void hubServiceTest() {

        DeviceAddedEvent deviceAddedEvent = new DeviceAddedEvent();
        deviceAddedEvent.setId("1");
        deviceAddedEvent.setDeviceType(DeviceType.CLIMATE_SENSOR);
        deviceAddedEvent.setTimestamp(Instant.now());
        deviceAddedEvent.setHubId("1");

        hubServise.sendHubEvent(deviceAddedEvent);

        ArgumentCaptor<ProducerRecord<String, SpecificRecordBase>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer, times(1)).send(captor.capture());

        ProducerRecord<String, SpecificRecordBase> producerRecord = captor.getValue();
        assertThat(producerRecord).isNotNull();
        assertThat(producerRecord.topic()).isEqualTo(kafkaProperties.getHubEventTopic());
        assertThat(producerRecord.key()).isEqualTo(deviceAddedEvent.getHubId());
        assertThat(producerRecord.value()).isNotNull();
    }
}
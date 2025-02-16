package ru.practicum.service.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.practicum.dto.hub.HubEvent;
import ru.practicum.mapper.hub.HubMapper;
import ru.practicum.util.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Service
@Slf4j
@RequiredArgsConstructor
public class HubServiceImpl implements HubServise {

    private final Producer<String, SpecificRecordBase> producer;

    private final KafkaProperties kafkaProperties;

    @Override
    public void sendHubEvent(HubEvent hubEvent) {

        HubEventAvro hubEventAvro = HubMapper.mapToHubEventAvro(hubEvent);

        producer.send(new ProducerRecord<>(kafkaProperties.getHubEventTopic(),
                null,
                hubEvent.getTimestamp().toEpochMilli(),
                hubEvent.getHubId(),
                hubEventAvro));

        log.info("Kafka message HubEvent : {}", hubEvent.getHubId());
        log.debug("Kafka message HubEvent = {}, {}", hubEvent.getHubId(), hubEventAvro);
    }
}

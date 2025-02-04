package ru.practicum.mapper;

import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.dto.hub.HubEvent;
import ru.practicum.dto.hub.HubEventType;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class HubMapper {

    public static HubEventAvro mapToHubEventAvro(HubEvent hubEvent) {
        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setTimestamp(hubEvent.getTimestamp())
                .setPayload(mapToHubSpecificRecordBase(hubEvent, hubEvent.getType()))
                .build();
    }


    public static SpecificRecordBase mapToHubSpecificRecordBase(HubEvent hubEvent, HubEventType strategy) {
        return strategy.mapToHubSpecificRecordBase(hubEvent);
    }
}

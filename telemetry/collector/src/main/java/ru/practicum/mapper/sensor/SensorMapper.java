package ru.practicum.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.dto.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class SensorMapper {

    public static SensorEventAvro mapToSensorEventAvro(SensorEvent sensorEvent) {
        return SensorEventAvro.newBuilder()
                .setId(sensorEvent.getId())
                .setHubId(sensorEvent.getHubId())
                .setTimestamp(sensorEvent.getTimestamp())
                .setPayload(mapToHubSpecificRecordBase(sensorEvent, sensorEvent.getType()))
                .build();
    }

    public static SpecificRecordBase mapToHubSpecificRecordBase(SensorEvent sensorEvent, SensorEventType strategy) {
        return strategy.mapToSensorSpecificRecordBase(sensorEvent);
    }
}

package ru.practicum.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.service.sensor.SensorService;
import ru.practicum.dto.sensor.LightSensorEvent;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class LightSensorEventHandler implements SensorEventHandler {

    private final SensorService sensorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {

        LightSensorEventProto lightSensorEventProto = event.getLightSensorEvent();

        LightSensorEvent lightSensorEvent = new LightSensorEvent();
        lightSensorEvent.setLinkQuality(lightSensorEventProto.getLinkQuality());
        lightSensorEvent.setId(event.getId());
        lightSensorEvent.setLuminosity(lightSensorEventProto.getLuminosity());
        lightSensorEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        lightSensorEvent.setHubId(event.getHubId());

        sensorService.sendSensorEvent(lightSensorEvent);

    }
}
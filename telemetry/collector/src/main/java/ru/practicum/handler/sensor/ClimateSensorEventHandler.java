package ru.practicum.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.dto.sensor.ClimateSensorEvent;
import ru.practicum.service.sensor.SensorService;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class ClimateSensorEventHandler implements SensorEventHandler {

    private final SensorService sensorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {

        ClimateSensorEventProto climateSensorEventProto = event.getClimateSensorEvent();

        ClimateSensorEvent climateSensorEvent = new ClimateSensorEvent();
        climateSensorEvent.setTemperatureC(climateSensorEventProto.getTemperatureC());
        climateSensorEvent.setId(event.getId());
        climateSensorEvent.setHumidity(climateSensorEventProto.getHumidity());
        climateSensorEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        climateSensorEvent.setCo2Level(climateSensorEventProto.getCo2Level());
        climateSensorEvent.setHubId(event.getHubId());

        sensorService.sendSensorEvent(climateSensorEvent);

    }
}
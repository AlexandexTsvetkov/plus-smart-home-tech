package ru.practicum.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.dto.sensor.TemperatureSensorEvent;
import ru.practicum.service.sensor.SensorService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class TemperatureSensorEventHandler implements SensorEventHandler {

    private final SensorService sensorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {

        TemperatureSensorEventProto temperatureSensorEventproto = event.getTemperatureSensorEvent();

        TemperatureSensorEvent temperatureSensorEvent = new TemperatureSensorEvent();
        temperatureSensorEvent.setTemperatureC(temperatureSensorEventproto.getTemperatureC());
        temperatureSensorEvent.setId(event.getId());
        temperatureSensorEvent.setTemperatureF(temperatureSensorEventproto.getTemperatureF());
        temperatureSensorEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        temperatureSensorEvent.setHubId(event.getHubId());

        sensorService.sendSensorEvent(temperatureSensorEvent);

    }
}
package ru.practicum.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.dto.sensor.SwitchSensorEvent;
import ru.practicum.service.sensor.SensorService;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class SwitchSensorEventHandler implements SensorEventHandler {

    private final SensorService sensorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {

        SwitchSensorEventProto switchSensorEventProto = event.getSwitchSensorEvent();

        SwitchSensorEvent switchSensorEvent = new SwitchSensorEvent();
        switchSensorEvent.setState(switchSensorEventProto.getState());
        switchSensorEvent.setId(event.getId());
        switchSensorEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        switchSensorEvent.setHubId(event.getHubId());

        sensorService.sendSensorEvent(switchSensorEvent);

    }
}
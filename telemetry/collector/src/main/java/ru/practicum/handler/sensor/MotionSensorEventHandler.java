package ru.practicum.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.dto.sensor.MotionSensorEvent;
import ru.practicum.service.sensor.SensorService;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class MotionSensorEventHandler implements SensorEventHandler {

    private final SensorService sensorService;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {

        MotionSensorEventProto motionSensorEventProto = event.getMotionSensorEvent();

        MotionSensorEvent motionSensorEvent = new MotionSensorEvent();
        motionSensorEvent.setMotion(motionSensorEventProto.getMotion());
        motionSensorEvent.setId(event.getId());
        motionSensorEvent.setVoltage(motionSensorEventProto.getVoltage());
        motionSensorEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        motionSensorEvent.setLinkQuality(motionSensorEventProto.getLinkQuality());
        motionSensorEvent.setHubId(event.getHubId());

        sensorService.sendSensorEvent(motionSensorEvent);

    }
}
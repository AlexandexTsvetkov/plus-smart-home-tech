package ru.practicum.mapper;

import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.dto.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class SensorMapper {

    public static SensorEventAvro mapToSensorEventAvro(SensorEvent sensorEvent) {
        return SensorEventAvro.newBuilder()
                .setId(sensorEvent.getId())
                .setHubId(sensorEvent.getHubId())
                .setTimestamp(sensorEvent.getTimestamp())
                .setPayload(mapToSpecificRecordBase(sensorEvent))
                .build();
    }

    public static SpecificRecordBase mapToSpecificRecordBase(SensorEvent sensorEvent) {

        switch (sensorEvent.getType()) {
            case SensorEventType.MOTION_SENSOR_EVENT -> {
                MotionSensorEvent motionSensorEvent = (MotionSensorEvent) sensorEvent;
                return MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionSensorEvent.getLinkQuality())
                        .setMotion(motionSensorEvent.isMotion())
                        .setVoltage(motionSensorEvent.getVoltage())
                        .build();

            }
            case SensorEventType.CLIMATE_SENSOR_EVENT -> {
                ClimateSensorEvent climateSensorEvent = (ClimateSensorEvent) sensorEvent;
                return ClimateSensorAvro.newBuilder()
                        .setCo2Level(climateSensorEvent.getCo2Level())
                        .setHumidity(climateSensorEvent.getHumidity())
                        .setTemperatureC(climateSensorEvent.getTemperatureC())
                        .build();

            }
            case SensorEventType.LIGHT_SENSOR_EVENT -> {
                LightSensorEvent lightSensorEvent = (LightSensorEvent) sensorEvent;
                return LightSensorAvro.newBuilder()
                        .setLinkQuality(lightSensorEvent.getLinkQuality())
                        .setLuminosity(lightSensorEvent.getLuminosity())
                        .build();

            }
            case SensorEventType.TEMPERATURE_SENSOR_EVENT -> {
                TemperatureSensorEvent temperatureSensorEvent = (TemperatureSensorEvent) sensorEvent;
                return TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(temperatureSensorEvent.getTemperatureC())
                        .setTemperatureF(temperatureSensorEvent.getTemperatureF())
                        .build();

            }
            case SensorEventType.SWITCH_SENSOR_EVENT -> {
                SwitchSensorEvent switchSensorEvent = (SwitchSensorEvent) sensorEvent;
                return SwitchSensorAvro.newBuilder()
                        .setState(switchSensorEvent.isState())
                        .build();

            }
            default -> throw new IllegalArgumentException("Unsupported sensor event type: " + sensorEvent.getType());
        }


    }
}

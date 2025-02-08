package ru.practicum.dto.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.event.*;

public enum SensorEventType {
    LIGHT_SENSOR_EVENT {
        @Override
        public SpecificRecordBase mapToSensorSpecificRecordBase(SensorEvent sensorEvent) {
            LightSensorEvent lightSensorEvent = (LightSensorEvent) sensorEvent;
            return LightSensorAvro.newBuilder()
                    .setLinkQuality(lightSensorEvent.getLinkQuality())
                    .setLuminosity(lightSensorEvent.getLuminosity())
                    .build();
        }
    },
    SWITCH_SENSOR_EVENT {
        @Override
        public SpecificRecordBase mapToSensorSpecificRecordBase(SensorEvent sensorEvent) {
            SwitchSensorEvent switchSensorEvent = (SwitchSensorEvent) sensorEvent;
            return SwitchSensorAvro.newBuilder()
                    .setState(switchSensorEvent.isState())
                    .build();
        }
    },
    CLIMATE_SENSOR_EVENT {
        @Override
        public SpecificRecordBase mapToSensorSpecificRecordBase(SensorEvent sensorEvent) {
            ClimateSensorEvent climateSensorEvent = (ClimateSensorEvent) sensorEvent;
            return ClimateSensorAvro.newBuilder()
                    .setCo2Level(climateSensorEvent.getCo2Level())
                    .setHumidity(climateSensorEvent.getHumidity())
                    .setTemperatureC(climateSensorEvent.getTemperatureC())
                    .build();
        }
    },
    MOTION_SENSOR_EVENT {
        @Override
        public SpecificRecordBase mapToSensorSpecificRecordBase(SensorEvent sensorEvent) {
            MotionSensorEvent motionSensorEvent = (MotionSensorEvent) sensorEvent;
            return MotionSensorAvro.newBuilder()
                    .setLinkQuality(motionSensorEvent.getLinkQuality())
                    .setMotion(motionSensorEvent.isMotion())
                    .setVoltage(motionSensorEvent.getVoltage())
                    .build();
        }
    },
    TEMPERATURE_SENSOR_EVENT {
        @Override
        public SpecificRecordBase mapToSensorSpecificRecordBase(SensorEvent sensorEvent) {
            TemperatureSensorEvent temperatureSensorEvent = (TemperatureSensorEvent) sensorEvent;
            return TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(temperatureSensorEvent.getTemperatureC())
                    .setTemperatureF(temperatureSensorEvent.getTemperatureF())
                    .build();
        }
    };

    public abstract SpecificRecordBase mapToSensorSpecificRecordBase(SensorEvent sensorEvent);
}

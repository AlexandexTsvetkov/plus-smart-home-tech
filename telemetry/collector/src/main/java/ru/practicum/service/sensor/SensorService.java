package ru.practicum.service.sensor;

import ru.practicum.dto.sensor.SensorEvent;

public interface SensorService {
    void sendSensorEvent(SensorEvent sensorEvent);
}

package ru.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.dto.hub.device.DeviceAddedEvent;
import ru.practicum.dto.hub.device.DeviceType;
import ru.practicum.service.hub.HubServise;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler {

    private final HubServise hubServise;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {

        DeviceAddedEventProto deviceAddedEventProto = event.getDeviceAdded();

        DeviceAddedEvent deviceAddedEvent = new DeviceAddedEvent();
        deviceAddedEvent.setDeviceType(DeviceType.valueOf(deviceAddedEventProto.getType().name()));
        deviceAddedEvent.setId(deviceAddedEventProto.getId());
        deviceAddedEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        deviceAddedEvent.setHubId(event.getHubId());

        hubServise.sendHubEvent(deviceAddedEvent);

    }
}
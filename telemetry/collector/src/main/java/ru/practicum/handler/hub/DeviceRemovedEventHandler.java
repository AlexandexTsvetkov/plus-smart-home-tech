package ru.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.dto.hub.device.DeviceRemovedEvent;
import ru.practicum.service.hub.HubServise;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class DeviceRemovedEventHandler implements HubEventHandler {

    private final HubServise hubServise;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    public void handle(HubEventProto event) {

        DeviceRemovedEventProto deviceRemovedEventProto = event.getDeviceRemoved();

        DeviceRemovedEvent deviceRemovedEvent = new DeviceRemovedEvent();
        deviceRemovedEvent.setId(deviceRemovedEventProto.getId());
        deviceRemovedEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        deviceRemovedEvent.setHubId(event.getHubId());

        hubServise.sendHubEvent(deviceRemovedEvent);

    }
}
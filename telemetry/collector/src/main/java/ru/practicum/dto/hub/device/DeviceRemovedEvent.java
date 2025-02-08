package ru.practicum.dto.hub.device;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.dto.hub.HubEventType;

@Getter
@Setter
@ToString(callSuper = true)
public class DeviceRemovedEvent extends DeviceEvent {

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_REMOVED_EVENT;
    }
}
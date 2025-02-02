package ru.practicum.dto.hub.device;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.dto.hub.HubEvent;

@Getter
@Setter
@ToString(callSuper = true)
public abstract class DeviceEvent extends HubEvent {
    private String id;
}

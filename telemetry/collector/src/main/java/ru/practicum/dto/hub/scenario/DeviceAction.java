package ru.practicum.dto.hub.scenario;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class DeviceAction {

    @NotNull
    private String sensorId;
    @NotNull
    private ActionType type;
    private int value;

}

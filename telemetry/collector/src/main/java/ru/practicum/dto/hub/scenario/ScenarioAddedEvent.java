package ru.practicum.dto.hub.scenario;

import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.dto.hub.HubEventType;

import java.util.List;

@Getter
@Setter
@ToString(callSuper = true)
public class ScenarioAddedEvent extends ScenarioEvent {

    @NotEmpty
    private List<DeviceAction> actions;

    @NotEmpty
    private List<ScenarioCondition> ScenarioCondition;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED_EVENT;
    }
}

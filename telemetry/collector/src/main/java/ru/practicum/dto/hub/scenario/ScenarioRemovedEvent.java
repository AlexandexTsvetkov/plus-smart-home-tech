package ru.practicum.dto.hub.scenario;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.dto.hub.HubEventType;

@Getter
@Setter
@ToString(callSuper = true)
public class ScenarioRemovedEvent extends ScenarioEvent {

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED_EVENT;
    }
}

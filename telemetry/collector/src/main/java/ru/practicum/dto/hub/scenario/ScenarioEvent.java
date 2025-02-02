package ru.practicum.dto.hub.scenario;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.dto.hub.HubEvent;

@Getter
@Setter
@ToString(callSuper = true)
public abstract class ScenarioEvent extends HubEvent {

    @NotNull
    private String name;
}

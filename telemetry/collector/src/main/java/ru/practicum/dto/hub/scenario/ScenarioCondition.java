package ru.practicum.dto.hub.scenario;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.dto.hub.OperationType;

@Getter
@Setter
@ToString
public class ScenarioCondition {

    @NotNull
    private String sensorId;

    @NotNull
    private ConditionType type;

    @NotNull
    private OperationType operation;

    private int value;
}

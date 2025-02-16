package ru.practicum.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.dto.hub.HubEvent;
import ru.practicum.dto.hub.HubEventType;
import ru.practicum.dto.hub.OperationType;
import ru.practicum.dto.hub.scenario.ActionType;
import ru.practicum.dto.hub.scenario.ConditionType;
import ru.practicum.dto.hub.scenario.DeviceAction;
import ru.practicum.dto.hub.scenario.ScenarioCondition;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class HubMapper {

    public static HubEventAvro mapToHubEventAvro(HubEvent hubEvent) {
        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setTimestamp(hubEvent.getTimestamp())
                .setPayload(mapToHubSpecificRecordBase(hubEvent, hubEvent.getType()))
                .build();
    }


    public static SpecificRecordBase mapToHubSpecificRecordBase(HubEvent hubEvent, HubEventType strategy) {
        return strategy.mapToHubSpecificRecordBase(hubEvent);
    }

    public static DeviceAction deviceActionFromProto(DeviceActionProto proto) {
        DeviceAction action = new DeviceAction();
        action.setSensorId(proto.getSensorId());
        action.setType(ActionType.valueOf(proto.getType().name()));
        action.setValue(proto.getValue());
        return action;
    }

    public static ScenarioCondition scenarioConditionFromProto(ScenarioConditionProto proto) {
        ScenarioCondition condition = new ScenarioCondition();
        condition.setSensorId(proto.getSensorId());
        condition.setType(ConditionType.valueOf(proto.getType().name()));
        condition.setOperation(OperationType.valueOf(proto.getOperation().name()));
        condition.setValue(proto.getIntValue());
        return condition;
    }
}

package ru.practicum.mapper;

import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.dto.hub.HubEvent;
import ru.practicum.dto.hub.HubEventType;
import ru.practicum.dto.hub.OperationType;
import ru.practicum.dto.hub.device.DeviceAddedEvent;
import ru.practicum.dto.hub.device.DeviceRemovedEvent;
import ru.practicum.dto.hub.device.DeviceType;
import ru.practicum.dto.hub.scenario.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

public class HubMapper {

    public static HubEventAvro mapToHubEventAvro(HubEvent hubEvent) {
        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setTimestamp(hubEvent.getTimestamp())
                .setPayload(mapToSpecificRecordBase(hubEvent))
                .build();
    }


    public static SpecificRecordBase mapToSpecificRecordBase(HubEvent hubEvent) {

        switch (hubEvent.getType()) {
            case HubEventType.DEVICE_ADDED_EVENT -> {
                DeviceAddedEvent deviceAddedEvent = (DeviceAddedEvent) hubEvent;
                return DeviceAddedEventAvro.newBuilder()
                        .setId(deviceAddedEvent.getId())
                        .setType(mapToDeviceTypeAvro(deviceAddedEvent.getDeviceType()))
                        .build();

            }
            case HubEventType.DEVICE_REMOVED_EVENT -> {
                DeviceRemovedEvent deviceRemovedEvent = (DeviceRemovedEvent) hubEvent;
                return DeviceRemovedEventAvro.newBuilder()
                        .setId(deviceRemovedEvent.getId())
                        .build();

            }
            case HubEventType.SCENARIO_ADDED_EVENT -> {
                ScenarioAddedEvent scenarioAddedEvent = (ScenarioAddedEvent) hubEvent;
                return ScenarioAddedEventAvro.newBuilder()
                        .setConditions(mapToConditionsTypeAvro(scenarioAddedEvent.getScenarioCondition()))
                        .setActions(mapToDeviceActionsAvro(scenarioAddedEvent.getActions()))
                        .build();

            }
            default -> throw new IllegalArgumentException("Unsupported hub event type: " + hubEvent.getType());
        }


    }
    public static ActionTypeAvro mapToActionTypeAvro(ActionType actionType) {
        return ActionTypeAvro.valueOf(actionType.name());
    }

    public static DeviceTypeAvro mapToDeviceTypeAvro(DeviceType deviceType) {
       return DeviceTypeAvro.valueOf(deviceType.name());
    }


    public static ConditionTypeAvro mapToConditionTypeAvro(ConditionType conditionType) {
        return ConditionTypeAvro.valueOf(conditionType.name());
    }

    public static List<ScenarioConditionAvro> mapToConditionsTypeAvro(List<ScenarioCondition> conditions) {
        return conditions.stream().map(HubMapper::mapToScenarioConditionAvro).toList();
    }

    public static ConditionOperationAvro mapToConditionOperationAvro(OperationType operationType) {
        return ConditionOperationAvro.valueOf(operationType.name());
    }

    public static ScenarioConditionAvro mapToScenarioConditionAvro(ScenarioCondition scenarioCondition) {
        return ScenarioConditionAvro.newBuilder()
                .setType(mapToConditionTypeAvro(scenarioCondition.getType()))
                .setOperation(mapToConditionOperationAvro(scenarioCondition.getOperation()))
                .setValue(scenarioCondition.getValue())
                .setSensorId(scenarioCondition.getSensorId())
                .build();
    }

    public static List<DeviceActionAvro> mapToDeviceActionsAvro(List<DeviceAction> deviceActions) {
        return deviceActions.stream().map(HubMapper::mapToDeviceActionAvro).toList();
    }

    public static DeviceActionAvro mapToDeviceActionAvro(DeviceAction deviceAction) {
            return DeviceActionAvro.newBuilder()
                    .setType(mapToActionTypeAvro(deviceAction.getType()))
                    .setSensorId(deviceAction.getSensorId())
                    .setValue(deviceAction.getValue())
                    .build();
    }

}

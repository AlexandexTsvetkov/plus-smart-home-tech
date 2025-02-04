package ru.practicum.dto.hub;

import org.apache.avro.specific.SpecificRecordBase;
import ru.practicum.dto.hub.device.DeviceAddedEvent;
import ru.practicum.dto.hub.device.DeviceRemovedEvent;
import ru.practicum.dto.hub.scenario.ScenarioAddedEvent;
import ru.practicum.dto.hub.scenario.ScenarioRemovedEvent;
import ru.yandex.practicum.kafka.telemetry.event.*;

public enum HubEventType {
    DEVICE_ADDED_EVENT {
        @Override
        public SpecificRecordBase mapToHubSpecificRecordBase(HubEvent hubEvent) {
            DeviceAddedEvent deviceAddedEvent = (DeviceAddedEvent) hubEvent;
            return DeviceAddedEventAvro.newBuilder()
                    .setId(deviceAddedEvent.getId())
                    .setType(DeviceTypeAvro.valueOf((deviceAddedEvent.getDeviceType().name())))
                    .build();
        }
    },
    DEVICE_REMOVED_EVENT {
        @Override
        public SpecificRecordBase mapToHubSpecificRecordBase(HubEvent hubEvent) {
            DeviceRemovedEvent deviceRemovedEvent = (DeviceRemovedEvent) hubEvent;
            return DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceRemovedEvent.getId())
                    .build();
        }
    },
    SCENARIO_ADDED_EVENT {
        @Override
        public SpecificRecordBase mapToHubSpecificRecordBase(HubEvent hubEvent) {
            ScenarioAddedEvent scenarioAddedEvent = (ScenarioAddedEvent) hubEvent;
            return ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioAddedEvent.getName())
                    .setConditions(scenarioAddedEvent.getConditions().stream().map(scenarioCondition -> ScenarioConditionAvro.newBuilder()
                            .setType(ConditionTypeAvro.valueOf(scenarioCondition.getType().name()))
                            .setOperation(ConditionOperationAvro.valueOf(scenarioCondition.getOperation().name()))
                            .setValue(scenarioCondition.getValue())
                            .setSensorId(scenarioCondition.getSensorId())
                            .build()).toList())
                    .setActions(scenarioAddedEvent.getActions().stream().map(deviceAction -> DeviceActionAvro.newBuilder()
                            .setType(ActionTypeAvro.valueOf(deviceAction.getType().name()))
                            .setSensorId(deviceAction.getSensorId())
                            .setValue(deviceAction.getValue())
                            .build()).toList())
                    .build();
        }
    },
    SCENARIO_REMOVED_EVENT {
        @Override
        public SpecificRecordBase mapToHubSpecificRecordBase(HubEvent hubEvent) {
            ScenarioRemovedEvent scenarioRemovedEvent = (ScenarioRemovedEvent) hubEvent;
            return ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenarioRemovedEvent.getName())
                    .build();
        }
    };

    public abstract SpecificRecordBase mapToHubSpecificRecordBase(HubEvent hubEvent);
}

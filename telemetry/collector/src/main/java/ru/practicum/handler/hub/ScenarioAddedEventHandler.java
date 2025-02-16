package ru.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.dto.hub.scenario.ScenarioAddedEvent;
import ru.practicum.mapper.hub.HubMapper;
import ru.practicum.service.hub.HubServise;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {

    private final HubServise hubServise;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {

        ScenarioAddedEventProto scenarioAddedEventProto = event.getScenarioAdded();

        ScenarioAddedEvent scenarioAddedEvent = new ScenarioAddedEvent();
        scenarioAddedEvent.setActions(scenarioAddedEventProto.getActionList().stream()
                .map(HubMapper::deviceActionFromProto)
                .toList());
        scenarioAddedEvent.setName(scenarioAddedEventProto.getName());
        scenarioAddedEvent.setConditions(scenarioAddedEventProto.getConditionList().stream()
                .map(HubMapper::scenarioConditionFromProto)
                .toList());
        scenarioAddedEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        scenarioAddedEvent.setHubId(event.getHubId());

        hubServise.sendHubEvent(scenarioAddedEvent);

    }
}
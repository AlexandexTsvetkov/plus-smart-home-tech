package ru.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.practicum.dto.hub.scenario.ScenarioRemovedEvent;
import ru.practicum.service.hub.HubServise;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler {

    private final HubServise hubServise;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    public void handle(HubEventProto event) {

        ScenarioRemovedEventProto scenarioRemovedEventProto = event.getScenarioRemoved();

        ScenarioRemovedEvent scenarioRemovedEvent = new ScenarioRemovedEvent();
        scenarioRemovedEvent.setName(scenarioRemovedEventProto.getName());
        scenarioRemovedEvent.setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        scenarioRemovedEvent.setHubId(event.getHubId());

        hubServise.sendHubEvent(scenarioRemovedEvent);

    }
}
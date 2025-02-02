package ru.practicum.service.hub;

import ru.practicum.dto.hub.HubEvent;

public interface HubServise {
    void sendHubEvent(HubEvent hubEvent);
}

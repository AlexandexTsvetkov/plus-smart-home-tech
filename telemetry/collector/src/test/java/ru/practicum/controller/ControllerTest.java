package ru.practicum.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import ru.practicum.dto.hub.device.DeviceAddedEvent;
import ru.practicum.dto.hub.device.DeviceType;
import ru.practicum.dto.sensor.ClimateSensorEvent;
import ru.practicum.service.hub.HubServise;
import ru.practicum.service.sensor.SensorService;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.times;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CollectorController.class)
public class ControllerTest {

    @MockBean
    private SensorService sensorService;

    @MockBean
    private HubServise hubServise;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void sensorControllerTest() throws Exception {
        Instant now = Instant.now();

        ClimateSensorEvent climateSensorEvent = new ClimateSensorEvent();
        climateSensorEvent.setId("1");
        climateSensorEvent.setHumidity(10);
        climateSensorEvent.setCo2Level(5);
        climateSensorEvent.setTimestamp(now);
        climateSensorEvent.setHubId("2");
        climateSensorEvent.setTemperatureC(4);

        mvc.perform(post("/sensors")
                        .content(objectMapper.writeValueAsString(climateSensorEvent))
                        .characterEncoding(StandardCharsets.UTF_8)
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        ArgumentCaptor<ClimateSensorEvent> requestSensorEvent = ArgumentCaptor.forClass(ClimateSensorEvent.class);
        Mockito.verify(sensorService, times(1)).sendSensorEvent(requestSensorEvent.capture());

        assertThat(requestSensorEvent.getValue()).isNotNull();
        ClimateSensorEvent capturedArgument = requestSensorEvent.getValue();
        assertThat(capturedArgument.getId()).isNotNull();
        assertThat(capturedArgument.getId()).isEqualTo("1");
        assertThat(capturedArgument.getHumidity()).isEqualTo(10);
        assertThat(capturedArgument.getCo2Level()).isEqualTo(5);
        assertThat(capturedArgument.getTimestamp()).isEqualTo(now);
        assertThat(capturedArgument.getHubId()).isEqualTo("2");
        assertThat(capturedArgument.getTemperatureC()).isEqualTo(4);
    }

    @Test
    public void hubControllerTest() throws Exception {
        Instant now = Instant.now();

        DeviceAddedEvent deviceAddedEvent = new DeviceAddedEvent();
        deviceAddedEvent.setId("1");
        deviceAddedEvent.setDeviceType(DeviceType.CLIMATE_SENSOR);
        deviceAddedEvent.setTimestamp(now);
        deviceAddedEvent.setHubId("1");

        mvc.perform(post("/hubs")
                        .content(objectMapper.writeValueAsString(deviceAddedEvent))
                        .characterEncoding(StandardCharsets.UTF_8)
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        ArgumentCaptor<DeviceAddedEvent> requestHubEvent = ArgumentCaptor.forClass(DeviceAddedEvent.class);
        Mockito.verify(hubServise, times(1)).sendHubEvent(requestHubEvent.capture());

        assertThat(requestHubEvent.getValue()).isNotNull();
        DeviceAddedEvent capturedArgument = requestHubEvent.getValue();
        assertThat(capturedArgument.getId()).isNotNull();
        assertThat(capturedArgument.getId()).isEqualTo("1");
        assertThat(capturedArgument.getHubId()).isEqualTo("1");
        assertThat(capturedArgument.getTimestamp()).isEqualTo(now);
        assertThat(capturedArgument.getDeviceType()).isEqualTo(DeviceType.CLIMATE_SENSOR);
    }
}

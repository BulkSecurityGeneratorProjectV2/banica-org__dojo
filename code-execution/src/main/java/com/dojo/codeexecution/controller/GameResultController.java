package com.dojo.codeexecution.controller;

import com.dojo.codeexecution.config.CodenjoyConfigProperties;
import com.dojo.codeexecution.model.TestResult;
import com.dojo.codeexecution.service.docker.DockerServiceImpl;
import com.dojo.codeexecution.service.grpc.UpdateScoreService;
import com.dojo.codeexecution.service.grpc.handler.DockerEventUpdateHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class GameResultController {
    private final DockerEventUpdateHandler dockerEventUpdateHandler;
    private final DockerServiceImpl dockerService;

    private final RestTemplate restTemplate;
    private final CodenjoyConfigProperties codenjoyConfigProperties;
    private final UpdateScoreService updateScoreService;

    @Autowired
    public GameResultController(DockerEventUpdateHandler dockerEventUpdateHandler, DockerServiceImpl dockerService,
                                RestTemplate restTemplate, CodenjoyConfigProperties codenjoyConfigProperties, UpdateScoreService updateScoreService) {
        this.dockerEventUpdateHandler = dockerEventUpdateHandler;
        this.dockerService = dockerService;
        this.restTemplate = restTemplate;
        this.codenjoyConfigProperties = codenjoyConfigProperties;
        this.updateScoreService = updateScoreService;
    }

    @PostMapping(path = "/test/result")
    public void testResult(@RequestBody TestResult testResult) {

        String usernameAndGame = testResult.getUsername();
        String username = getUsername(usernameAndGame);
        String game = getGame(usernameAndGame);
        int points = testResult.getPoints();

        updateScoreService.updateScore(username, game, points);
        dockerEventUpdateHandler.sendUpdate(usernameAndGame, testResult.getFailedTestCases());
        stopContainerIfRunning(testResult);
    }

    private void stopContainerIfRunning(TestResult testResult) {
        String containerId = testResult.getContainerId();
        if (dockerService.getContainerStatus(containerId).equals("running")) {
            dockerService.stopContainer(containerId);
        }
    }

    private String getUsername(String usernameAndGame) {
        String game = getGame(usernameAndGame);
        return usernameAndGame.substring(0, usernameAndGame.length() - (game.length() + 1));
    }

    private String getGame(String usernameAndGame) {
        String[] splitUsernameAndGame = usernameAndGame.split("-");
        return splitUsernameAndGame[splitUsernameAndGame.length - 1];
    }
}

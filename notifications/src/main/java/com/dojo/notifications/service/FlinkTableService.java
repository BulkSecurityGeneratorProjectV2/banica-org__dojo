package com.dojo.notifications.service;

import com.dojo.notifications.model.docker.Container;
import com.dojo.notifications.model.docker.TestResults;
import com.dojo.notifications.model.request.SelectRequest;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Service
public class FlinkTableService {

    private static final String LEADERBOARD = "leaderboard";
    private static final String DOCKER_EVENTS = "docker_events";

    private static final String CONTAINER_TYPE = "container";
    private static final String TEST_RESULTS_TYPE = "test_results";
    private static final String EMPTY = "";

    public List<String> executeDockerQuery(SelectRequest request, Object object) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = getStreamTableEnvironment(executionEnvironment);

        Tuple5<String, String, String, String, Integer> tuple5 = getTuple5(object);

        DataStream<Tuple5<String, String, String, String, Integer>> tuple5DataStreamSource = executionEnvironment.fromCollection(Collections.singletonList(tuple5));
        Table table = tableEnvironment.fromDataStream(tuple5DataStreamSource).as("type", "username", "container_status", "code_execution", "failed_test_cases");

        Table tableResult = executeSql(tableEnvironment, table, DOCKER_EVENTS, request);

        DataStream<Tuple5<String, String, String, String, Integer>> tupleStream = tableEnvironment.toAppendStream(
                tableResult,
                new TypeHint<Tuple5<String, String, String, String, Integer>>() {

                }.getTypeInfo()
        );

        return getUsernames(tupleStream.executeAndCollect());
    }

    public Set<String> executeLeaderboardQuery(SelectRequest request, List<Tuple4<String, String, Integer, Long>> changedUsers) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = getStreamTableEnvironment(executionEnvironment);

        DataStream<Tuple4<String, String, Integer, Long>> tuple4DataStream = executionEnvironment.fromCollection(changedUsers);
        Table table = tableEnvironment.fromDataStream(tuple4DataStream).as("id", "name", "place", "score");

        Table tableResult = executeSql(tableEnvironment, table, LEADERBOARD, request);

        DataStream<Tuple4<String, String, Integer, Long>> tupleStream = tableEnvironment.toAppendStream(
                tableResult,
                new TypeHint<Tuple4<String, String, Integer, Long>>() {
                }.getTypeInfo()
        );

        return convertDataStreamToSet(tupleStream.executeAndCollect());
    }

    private StreamTableEnvironment getStreamTableEnvironment(StreamExecutionEnvironment executionEnvironment) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        return StreamTableEnvironment.create(executionEnvironment, settings);
    }

    private List<String> getUsernames(Iterator<Tuple5<String, String, String, String, Integer>> result) {
        List<String> usernames = new ArrayList<>();
        result.forEachRemaining(event -> usernames.add(event.f1));
        return usernames;
    }

    private Set<String> convertDataStreamToSet(Iterator<Tuple4<String, String, Integer, Long>> leaderboard) {
        Set<String> userIds = new TreeSet<>();
        leaderboard.forEachRemaining(user -> userIds.add(user.f0));
        return userIds;
    }

    private Tuple5<String, String, String, String, Integer> getTuple5(Object object) {
        Tuple5<String, String, String, String, Integer> tuple5 = new Tuple5<>();

        if (object instanceof Container) {
            Container container = (Container) object;
            tuple5 = new Tuple5<>(CONTAINER_TYPE, container.getUsername(), container.getStatus(), container.getCodeExecution(), -1);
        }

        if (object instanceof TestResults) {
            TestResults testResults = (TestResults) object;
            tuple5 = new Tuple5<>(TEST_RESULTS_TYPE, testResults.getUsername(), EMPTY, EMPTY, testResults.getFailedTestCases().size());
        }
        return tuple5;
    }

    private Table executeSql(StreamTableEnvironment tableEnvironment, Table table, String tableName, SelectRequest request) {
        tableEnvironment.createTemporaryView(tableName, table);
        try {
            table = tableEnvironment.sqlQuery(request.getQuery());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        tableEnvironment.dropTemporaryView(tableName);
        return table;
    }
}

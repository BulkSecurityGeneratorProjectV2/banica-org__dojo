package com.dojo.notifications.service;

import com.dojo.notifications.model.docker.Container;
import com.dojo.notifications.model.docker.TestResults;
import com.dojo.notifications.model.leaderboard.SlackNotificationUtils;
import com.dojo.notifications.model.request.SelectRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import com.hubspot.slack.client.models.blocks.objects.Text;
import com.hubspot.slack.client.models.blocks.objects.TextType;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Service
public class FlinkTableService {

    private static final String LEADERBOARD = "leaderboard";
    private static final String DOCKER_EVENTS = "docker_events";

    private static final String CONTAINER_TYPE = "container";
    private static final String TEST_RESULTS_TYPE = "test_results";
    private static final String EMPTY = "";

    @Autowired
    private SelectRequestService selectRequestService;

    @Autowired
    private ActiveRequestsService activeRequestsService;

    private Map<String, List<Map<String, String>>> tables;

    @PostConstruct
    private void load() throws IOException {
        this.tables = new ObjectMapper().readValue(getFlinkTablesFile(), new TypeReference<Map<String, List<Map<String, String>>>>() {
        });
    }

    public Map<String, List<Map<String, String>>> getTables() {
        return Collections.unmodifiableMap(tables);
    }

    public List<TableColumn> getTableColumns(Table table) {
        return table.getSchema().getTableColumns();
    }

    public List<List<String>> getTableRows(Table table) {
        List<List<String>> rows = new ArrayList<>();
        table.execute().collect().forEachRemaining(row -> {
            List<String> fields = Arrays.asList(row.toString().split(",").clone());
            rows.add(fields);
        });
        return rows;
    }

    public Text buildColumnNames(List<TableColumn> tableColumns) {
        StringBuilder sb = new StringBuilder();
        tableColumns.forEach(column ->
                sb.append(SlackNotificationUtils.makeBold(column.getName())).append("\n"));
        return Text.of(TextType.MARKDOWN, String.valueOf(sb));
    }

    public Text buildFields(List<String> fields) {
        StringBuilder sb = new StringBuilder();
        fields.forEach(field ->
                sb.append(field).append("\n"));
        return Text.of(TextType.MARKDOWN, String.valueOf(sb));
    }

    public List<String> executeDockerQuery(SelectRequest request, Object object, String id) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = getStreamTableEnvironment(executionEnvironment);

        Tuple6<String, String, String, String, String, Integer> tuple6 = getTuple6(object, id);
        Table table = getDockerEventsTable(executionEnvironment, tableEnvironment, Collections.singletonList(tuple6));

        Table tableResult = executeSql(tableEnvironment, table, DOCKER_EVENTS, request);
        DataStream<Tuple6<String, String, String, String, String, Integer>> tupleStream = tableEnvironment.toAppendStream(
                tableResult,
                new TypeHint<Tuple6<String, String, String, String, String, Integer>>() {

                }.getTypeInfo()
        );

        return getDockerUserIds(tupleStream.executeAndCollect());
    }

    public Set<String> executeLeaderboardQuery(SelectRequest request, List<Tuple4<String, String, Integer, Long>> changedUsers) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = getStreamTableEnvironment(executionEnvironment);

        Table table = getLeaderboardTable(executionEnvironment, tableEnvironment, changedUsers);

        Table tableResult = executeSql(tableEnvironment, table, LEADERBOARD, request);
        DataStream<Tuple4<String, String, Integer, Long>> tupleStream = tableEnvironment.toAppendStream(
                tableResult,
                new TypeHint<Tuple4<String, String, Integer, Long>>() {
                }.getTypeInfo()
        );

        return getLeaderboardUserIds(tupleStream.executeAndCollect());
    }

    public Table executeLeaderboardJoinQuery(SelectRequest selectRequest, List<Tuple4<String, String, Integer, Long>> changedUsers, Map<String, List<Object>> dockerEvents) {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = getStreamTableEnvironment(executionEnvironment);

        Table leaderboardTable = getLeaderboardTable(executionEnvironment, tableEnvironment, changedUsers);

        List<Tuple6<String, String, String, String, String, Integer>> dockerTuples = new ArrayList<>();
        dockerEvents.forEach((id, list) -> {
            for (Object object : list) {
                dockerTuples.add(getTuple6(object, id));
            }
        });
        Table dockerEventsTable = getDockerEventsTable(executionEnvironment, tableEnvironment, dockerTuples);

        return executeJoinSql(tableEnvironment, leaderboardTable, LEADERBOARD, dockerEventsTable, DOCKER_EVENTS, selectRequest);
    }

    private Table getDockerEventsTable(StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment, List<Tuple6<String, String, String, String, String, Integer>> dockerTuples) {
        DataStream<Tuple6<String, String, String, String, String, Integer>> tuple6DataStreamSource = executionEnvironment.fromCollection(dockerTuples);
        List<String> dockerColumns = getColumnNamesForTable(DOCKER_EVENTS);
        String[] dockerColumnsWithoutFirst = getElementsFromFirstIndex(dockerColumns);
        return tableEnvironment.fromDataStream(tuple6DataStreamSource).as(dockerColumns.get(0), dockerColumnsWithoutFirst);
    }

    private Table getLeaderboardTable(StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment, List<Tuple4<String, String, Integer, Long>> changedUsers) {
        DataStream<Tuple4<String, String, Integer, Long>> tuple4DataStream = executionEnvironment.fromCollection(changedUsers);
        List<String> leaderboardColumns = getColumnNamesForTable(LEADERBOARD);
        String[] leaderboardColumnsWithoutFirst = getElementsFromFirstIndex(leaderboardColumns);
        return tableEnvironment.fromDataStream(tuple4DataStream).as(leaderboardColumns.get(0), leaderboardColumnsWithoutFirst);
    }

    private List<String> getColumnNamesForTable(String table) {
        List<String> columns = new ArrayList<>();
        this.tables.get(table).forEach(map -> columns.add(map.get("label")));
        return columns;
    }

    private String[] getElementsFromFirstIndex(List<String> columns) {
        String[] columnsWithoutFirst = new String[columns.size() - 1];
        if (columns.size() > 1) {
            columnsWithoutFirst = columns.subList(1, columns.size()).toArray(columnsWithoutFirst);
        }
        return columnsWithoutFirst;
    }

    private StreamTableEnvironment getStreamTableEnvironment(StreamExecutionEnvironment executionEnvironment) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        return StreamTableEnvironment.create(executionEnvironment, settings);
    }

    private List<String> getDockerUserIds(Iterator<Tuple6<String, String, String, String, String, Integer>> dockerEvent) {
        List<String> userIds = new ArrayList<>();
        dockerEvent.forEachRemaining(event -> userIds.add(event.f1));
        return userIds;
    }

    private Set<String> getLeaderboardUserIds(Iterator<Tuple4<String, String, Integer, Long>> leaderboard) {
        Set<String> userIds = new TreeSet<>();
        leaderboard.forEachRemaining(user -> userIds.add(user.f0));
        return userIds;
    }

    private Tuple6<String, String, String, String, String, Integer> getTuple6(Object object, String id) {
        Tuple6<String, String, String, String, String, Integer> tuple6 = new Tuple6<>();

        if (object instanceof Container) {
            Container container = (Container) object;
            tuple6 = new Tuple6<>(CONTAINER_TYPE, id, container.getUsername(), container.getStatus(), container.getCodeExecution(), -1);
        }

        if (object instanceof TestResults) {
            TestResults testResults = (TestResults) object;
            tuple6 = new Tuple6<>(TEST_RESULTS_TYPE, id, testResults.getUsername(), EMPTY, EMPTY, testResults.getFailedTestCases().size());
        }
        return tuple6;
    }

    private Table executeSql(StreamTableEnvironment tableEnvironment, Table table, String tableName, SelectRequest request) {
        tableEnvironment.createTemporaryView(tableName, table);
        try {
            table = tableEnvironment.sqlQuery(request.getQuery());
        } catch (Exception e) {
            e.printStackTrace();
            selectRequestService.deleteRequest(request.getId());
            activeRequestsService.removeRequestForAllContests(request.getId());
            return null;
        }
        tableEnvironment.dropTemporaryView(tableName);
        return table;
    }

    private Table executeJoinSql(StreamTableEnvironment tableEnvironment, Table firstTable, String firstTableName, Table secondTable, String secondTableName, SelectRequest selectRequest) {
        tableEnvironment.createTemporaryView(firstTableName, firstTable);
        tableEnvironment.createTemporaryView(secondTableName, secondTable);
        try {
            firstTable = tableEnvironment.sqlQuery(selectRequest.getQuery());
        } catch (Exception e) {
            e.printStackTrace();
            selectRequestService.deleteRequest(selectRequest.getId());
            activeRequestsService.removeRequestForAllContests(selectRequest.getId());
            return null;
        }
        tableEnvironment.dropTemporaryView(firstTableName);
        tableEnvironment.dropTemporaryView(secondTableName);
        return firstTable;
    }

    private File getFlinkTablesFile() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("static/flink-tables.json");
        File flinkTablesFile = Files.createTempFile("flink-tables", ".json").toFile();
        try {
            FileUtils.copyInputStreamToFile(inputStream, flinkTablesFile);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return flinkTablesFile;
    }
}

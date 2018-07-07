package com.nosql.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraConnectorTest {

    private Session session;
    private KeyspaceRepository keyspaceRepository;

    public void connect() {
        CassandraConnector cassandraConnector = new CassandraConnector();
        cassandraConnector.connect("13.250.238.236", 9142);

        this.session = cassandraConnector.getSession();
        keyspaceRepository = new KeyspaceRepository(this.session);
    }

    @Test
    public void shouldCreateKeyspace() {

        String keyspaceName = "library";

        keyspaceRepository.createKeyspace(keyspaceName, "Simple", 1);

        ResultSet resultSet = this.session.execute("select * from system_schema.keyspaces;");

        List<String> matchedKeyspaces = resultSet.all()
                .stream()
                .filter(r -> r.getString(0).equals(keyspaceName.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        matchedKeyspaces.forEach(System.out::println);
    }

    @Test
    public void shouldTest() {
        String test = "aaaaaaapplesafdapple";
        String matcher = "apple";
        List<Character> window = new ArrayList<>();
        int matches = 0;

        for (int index = 0; index < test.length(); index++) {
            if (window.size() >= matcher.length()) {
                if ("apple".equals(getWord(window))) {

                    matches++;
                }
                window.remove(0);
            }
            window.add(test.charAt(index));
        }
        System.out.println(matches);
    }

    private String getWord(List<Character> chars) {
        StringBuilder res = new StringBuilder();
        for (Character singleChar : chars) {
            res.append(singleChar);
        }
        return res.toString();
    }
}
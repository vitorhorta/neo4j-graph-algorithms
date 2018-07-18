/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class PersonalizedPageRankTest {

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{HeavyCypherGraphFactory.class, "HeavyCypherGraphFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"}
        );
    }
    private static final String DB_CYPHER = "" +
            "CREATE (john:Person {name:\"John\"})\n" +
            "CREATE (mary:Person {name:\"Mary\"})\n" +
            "CREATE (jill:Person {name:\"Jill\"})\n" +
            "CREATE (todd:Person {name:\"Todd\"})\n" +

            "CREATE (iphone:Product {name:\"iPhone5\"})\n" +
            "CREATE (kindle:Product {name:\"Kindle Fire\"})\n" +
            "CREATE (fitbit:Product {name:\"Fitbit Flex Wireless\"})\n" +
            "CREATE (potter:Product {name:\"Harry Potter\"})\n" +
            "CREATE (hobbit:Product {name:\"Hobbit\"})\n" +

            "CREATE\n" +
            "  (john)-[:PURCHASED]->(iphone),\n" +
            "  (john)<-[:PURCHASED_BY]-(iphone),\n" +

            "  (john)-[:PURCHASED]->(kindle),\n" +
            "  (john)<-[:PURCHASED_BY]-(kindle),\n" +

            "  (mary)-[:PURCHASED]->(iphone),\n" +
            "  (mary)<-[:PURCHASED_BY]-(iphone),\n" +

            "  (mary)-[:PURCHASED]->(kindle),\n" +
            "  (mary)<-[:PURCHASED_BY]-(kindle),\n" +

            "  (mary)-[:PURCHASED]->(fitbit),\n" +
            "  (mary)<-[:PURCHASED_BY]-(fitbit),\n" +

            "  (jill)-[:PURCHASED]->(iphone),\n" +
            "  (jill)<-[:PURCHASED_BY]-(iphone),\n" +

            "  (jill)-[:PURCHASED]->(kindle),\n" +
            "  (jill)<-[:PURCHASED_BY]-(kindle),\n" +

            "  (jill)-[:PURCHASED]->(fitbit),\n" +
            "  (jill)<-[:PURCHASED_BY]-(fitbit),\n" +

            "  (todd)-[:PURCHASED]->(fitbit),\n" +
            "  (todd)<-[:PURCHASED_BY]-(fitbit),\n" +

            "  (todd)-[:PURCHASED]->(potter),\n" +
            "  (todd)<-[:PURCHASED_BY]-(potter),\n" +

            "  (todd)-[:PURCHASED]->(hobbit),\n" +
            "  (todd)<-[:PURCHASED_BY]-(hobbit)";

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        if (db!=null) db.shutdown();
    }

    public PersonalizedPageRankTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void test() throws Exception {
        final Graph graph;
        if (graphImpl.isAssignableFrom(HeavyCypherGraphFactory.class)) {
            graph = new GraphLoader(db)
                    .withLabel("MATCH (n) RETURN id(n) as id")
                    .withRelationshipType("MATCH (n)-->(m) RETURN id(n) as source,id(m) as target")
                    .load(graphImpl);

        } else {
            graph = new GraphLoader(db)
                    .withDirection(Direction.BOTH)
                    .load(graphImpl);
        }

        Stream<Long> sourceNodeIds;
        try(Transaction tx = db.beginTx()) {
            Node node = db.findNode(Label.label("Person"), "name", "Mary");
            sourceNodeIds = Stream.of(node.getId());
        }


        final PageRankResult rankResult = PageRankAlgorithm
                .of(graph,0.85, sourceNodeIds, Pools.DEFAULT, 2, 1)
                .compute(40)
                .result();

        Map<String, Double> prs = new TreeMap<>();

        try(Transaction tx = db.beginTx()) {
            for (Node node : db.getAllNodes()) {
                double score = rankResult.score(node.getId());
                prs.put(node.getProperty("name").toString(), score);
            }
        }

        Map<String, Double> sortedPrs = sortByValue(prs);
        for (String name : sortedPrs.keySet()) {
            System.out.println(name + " => " +  sortedPrs.get(name));
        }

        /*

        Personalised PageRank
        John 0.2495885915
        iPhone5 0.1757435084
        Kindle Fire 0.1757435084
        Mary 0.1229457566
        Jill 0.1229457566
        Fitbit Flex Wireless 0.0824359888
        Todd 0.0450622296
        Harry Potter 0.0127673300
        Hobbit 0.0127673300

        MATCH (u:User {id: "Doug"})
WITH u, collect(u) AS sourceNodes
CALL algo.pageRank.stream('User', 'FOLLOWS', {
  iterations:20,
  dampingFactor:0.85,
  sourceNodes: sourceNodes
})
YIELD nodeId, score
MATCH (node)
WHERE id(node) = nodeId
AND node <> u
AND not((u)-[:FOLLOWS]->(node))
RETURN node.id AS page, score
ORDER BY score DESC
         */
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }
}

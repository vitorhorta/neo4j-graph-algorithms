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
package org.neo4j.graphalgo.algo;

import org.junit.*;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.ml.FeatureVectorsProc;
import org.neo4j.graphalgo.similarity.CosineProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.map;

public class FeatureVectorsConcurrencyTest {

    private static GraphDatabaseAPI db;
    private Transaction tx;
    public static final String STATEMENT_STREAM = "MATCH (i:Item) WITH collect(id(i)) AS items " +
            "MATCH (p:Person) " +
            "WITH items, {item:id(p), selectedValues: [(p)-[r:LIKES]->(item) | {item: id(item), weight: r.stars}]} as userData\n" +
            "WITH items, collect(userData) as data\n" +
            "call algo.ml.featureVector.stream(items, data,$config) " +
            "yield nodeId, featureVector " +
            "RETURN nodeId, featureVector " +
            "ORDER BY nodeId";

    @BeforeClass
    public static void beforeClass() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(FeatureVectorsProc.class);
    }

    @AfterClass
    public static void AfterClass() {
        db.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        tx = db.beginTx();
    }

    @After
    public void tearDown() throws Exception {
        tx.close();
    }

    private static void buildRandomDB(int size) {
        db.execute("MATCH (n) DETACH DELETE n").close();
        db.execute("UNWIND range(1,$size/10) as _ CREATE (:Person) CREATE (:Item) ",singletonMap("size",size)).close();
        String statement =
                "MATCH (p:Person) WITH collect(p) as people " +
                "MATCH (i:Item) WITH people, collect(i) as items " +
                "UNWIND range(1,$size) as _ " +
                "WITH people[toInteger(rand()*size(people))] as p, items[toInteger(rand()*size(items))] as i " +
                "MERGE (p)-[:LIKES {stars: rand()}]->(i) RETURN count(*) ";
        db.execute(statement,singletonMap("size",size)).close();
    }

    @Test
    public void multiThreadComparision() {
        int size = 333;
        buildRandomDB(size);
        Result result1 = db.execute(STATEMENT_STREAM, map("config", map("concurrency", 1)));
        Result result2 = db.execute(STATEMENT_STREAM, map("config", map("concurrency", 2)));
        Result result4 = db.execute(STATEMENT_STREAM, map("config", map("concurrency", 4)));
        Result result8 = db.execute(STATEMENT_STREAM, map("config", map("concurrency", 8)));
        int count=0;
        while (result1.hasNext()) {
            Map<String, Object> row1 = result1.next();
            assertEquals(row1.toString(), row1,result2.next());
            assertEquals(row1.toString(), row1,result4.next());
            assertEquals(row1.toString(), row1,result8.next());
            count++;
        }
        int people = size/10;
        assertEquals(people,count);
    }

}

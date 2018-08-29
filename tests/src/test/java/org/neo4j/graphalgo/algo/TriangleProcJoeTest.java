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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TriangleProc;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 *      (a)--(b)-(d)--(e)
 *        \T1/ \   \T2/
 *        (c)  (g)-(f)
 *          \  /T3\
 *           (h)-(i)
 *
 * @author mknblch
 */
public class TriangleProcJoeTest {

    public static final Label LABEL = Label.label("Node");
    private static GraphDatabaseAPI api;
    private static String[] idToName;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "UNWIND $people AS person\n" +
                "MERGE (:Person {name: person})";

        final String relationships = "unwind [\n" +
                "  ['Mark', 'Jennifer'],\n" +
                "  ['Mark', 'Joe'],\n" +
                "  ['Jennifer', 'Joe'] \n" +
                "\n" +
                "] AS pair\n" +
                "merge (p1:Person {name: pair[0]})\n" +
                "merge (p2:Person {name: pair[1]})\n" +
                "merge (p1)-[:KNOWS]-(p2)";

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(TriangleProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher, MapUtil.map("people", Arrays.asList("Mark", "Jennifer" ,"Michael", "Joe")));
            api.execute(relationships);
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        if (api != null) api.shutdown();
    }

    @Test
    public void testTriangleCountStream() throws Exception {
        final String cypher = "CALL algo.triangleCount.stream('Person', 'KNOWS', {concurrency:1}) YIELD nodeId, triangles";
        api.execute(cypher).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            System.out.println("nodeId = " + nodeId + ", " + triangles);
            return true;
        });
    }

    @Test
    public void testTriangleCountCypherStream() throws Exception {
        final String cypher = "CALL algo.triangleCount.stream(" +
                "'MATCH (p:Person) RETURN id(p) AS id', " +
                "'MATCH (p1:Person)-[:KNOWS]-(p2) RETURN id(p1) AS source, id(p2) AS target', " +
                "{concurrency:1, graph: 'cypher'}) YIELD nodeId, triangles";
        api.execute(cypher).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            System.out.println("nodeId = " + nodeId + ", " + triangles);
            return true;
        });
    }

    interface TriangleCountConsumer {
        void consume(long nodeId, long triangles);
    }

    interface TripleConsumer {
        void consume(String nodeA, String nodeB, String nodeC);
    }
}

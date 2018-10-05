/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 3, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AdjacencyMatrixBenchmark2 implements WeightedRelationshipConsumer {

    //    @Param({"HEAVY", "HUGE"})
    @Param({"HEAVY"})
    GraphImpl graph;


    private GraphDatabaseAPI db;
    private HeavyGraph heavyGraph;
    private double[] allTheWeights;

    @Setup
    public void setup() throws KernelException, IOException {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        String createGraph = "" +
                "UNWIND range(0,200000) AS id\n" +
                "MERGE (p1:Person {id: id})\n" +
                "MERGE (p2:Person {id: id+1})\n" +
                "MERGE (p3:Person {id: id+2})\n" +
                "MERGE (p4:Person {id: id+3})\n" +
                "MERGE (p5:Person {id: id+4})\n" +
                "MERGE (p6:Person {id: id+5})\n" +
                "MERGE (p1)-[:LINK {weight: 1}]->(p2)" +
                "MERGE (p1)-[:LINK {weight: 2}]->(p3)" +
                "MERGE (p1)-[:LINK {weight: 3}]->(p4)" +
                "MERGE (p1)-[:LINK {weight: 4}]->(p5)" +
                "MERGE (p1)-[:LINK {weight: 5}]->(p6)";

        db.execute("CREATE INDEX ON :Person(id)");

        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

        heavyGraph = (HeavyGraph) new GraphLoader(db, Pools.DEFAULT)
                .withDirection(Direction.OUTGOING)
                .withRelationshipWeightsFromProperty("weight", 1.0)
                .load(graph.impl);
    }

    @TearDown
    public void shutdown() {
        heavyGraph.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public double run() throws Exception {
        AdjacencyMatrix adjacencyMatrix = heavyGraph.container;

        long numberOfNodes = heavyGraph.nodeCount();

        allTheWeights = new double[]{0.0};
        for (int i = 0; i < numberOfNodes; i++) {
            adjacencyMatrix.forEach(i, Direction.OUTGOING, heavyGraph.relationshipWeights, this);
        }

        return allTheWeights[0];
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId, double weight) {
        allTheWeights[0] += weight;
        return true;

    }
}

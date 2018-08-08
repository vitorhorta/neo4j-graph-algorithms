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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.DeepWalkProc;
import org.neo4j.graphalgo.HarmonicCentralityProc;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;


/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class DeepWalkIntegrationTest {

    public static final String TYPE = "TYPE";

    @ClassRule
    public static final ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static DefaultBuilder builder;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        builder = GraphBuilder.create(db)
                .setLabel("Node")
                .setRelationship(TYPE);

        final RelationshipType type = RelationshipType.withName(TYPE);

        /**
         * create two rings of nodes where each node of ring A
         * is connected to center while center is connected to
         * each node of ring B.
         */
        final Node center = builder.newDefaultBuilder()
                .setLabel("Node")
                .createNode();

        builder.newRingBuilder()
                .createRing(5)
                .forEachNodeInTx(node -> {
                    node.createRelationshipTo(center, type);
                })
                .newRingBuilder()
                .createRing(5)
                .forEachNodeInTx(node -> {
                    center.createRelationshipTo(node, type);
                });

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(DeepWalkProc.class);
    }

    @Test
    public void testDeepWalkStream() throws Exception {

        db.execute("CALL algo.deepWalk.stream(null, null) YIELD nodeId, embedding")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    System.out.println("row = " + row.get("nodeId"));
                    System.out.println("row = " + row.get("embedding"));

                    return true;
                });
    }

    @Test
    public void testDeepWalk2Stream() throws Exception {

        db.execute("CALL algo.deepWalk2.stream(null, null) YIELD nodeId, embedding")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    System.out.println("row = " + row.get("nodeId"));
                    System.out.println("row = " + row.get("embedding"));

                    return true;
                });
    }


    @Test
    public void testDeepWalk() throws Exception {

        db.execute("CALL algo.deepWalk(null, null, {vectorSize: 100, windowSize: 10})");

        Result result = db.execute("MATCH (n) RETURN n.deepWalk AS deepWalk");
        Map<String, Object> row = result.next();

        System.out.println("row.get(\"deepWalk\") = " + Arrays.toString((double[]) row.get("deepWalk")));

    }

}

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
package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.Similarity;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.betweenness.ParallelBetweennessCentrality;
import org.neo4j.graphalgo.impl.betweenness.ParallelNearestNeighbors;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;
import weka.core.neighboursearch.KDTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 20.10.17
 */
public class SandboxTest {

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        DB.execute("FOREACH (x IN range(1, 4098) | " +
                "CREATE (n:Node {index:x}) " +
                "SET n.vector = [ _ in range(0, 100) | rand()])");
//        DB.execute("MATCH (n) WHERE n.index IN [1, 2, 3] DELETE n");
    }


    @Test
    public void list() throws Exception {
        DenseInstance one = createPoint(new double[]{1, 2, 3, 4, 5});
        DenseInstance two = createPoint(new double[]{1, 2, 4, 4, 5});
        DenseInstance three = createPoint(new double[]{1, 4, 3, 7, 5});

        ArrayList<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("0"));
        attributes.add(new Attribute("1"));
        attributes.add(new Attribute("2"));
        attributes.add(new Attribute("3"));
        attributes.add(new Attribute("4"));

        Instances instances = new Instances("Vectors", attributes, 3);

        instances.add(one);
        instances.add(two);
        instances.add(three);

        KDTree kdTree = new KDTree(instances);
        kdTree.setInstances(instances);

        Instances testInstances = new Instances("SingleVector", attributes, 1);
        testInstances.add(one);
        Instances closest = kdTree.kNearestNeighbours(testInstances.firstInstance(), 2);
        System.out.println("closest = " + closest);
    }

    @Test
    public void shouldNew() throws Exception {
        // given

        Graph graph = new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("MATCH (n:Node) RETURN id(n) AS id, n.vector AS vector")
                .withAnyRelationshipType()
                .withNodeVector("vector", new double[]{})
                .load(HeavyCypherGraphFactory.class);

        final ParallelNearestNeighbors nn = new ParallelNearestNeighbors(graph, Pools.DEFAULT, 8);
        nn.compute();

        Stream<ParallelNearestNeighbors.Result> stream = nn.resultStream();
        Iterator<ParallelNearestNeighbors.Result> iterator = stream.iterator();

        while (iterator.hasNext()) {
            System.out.println("iterator.next() = " + iterator.next());
        }
    }

    private DenseInstance createPoint(double[] point) {
        DenseInstance di = new DenseInstance(point.length);
        di.replaceMissingValues(point);
        return di;
    }


}

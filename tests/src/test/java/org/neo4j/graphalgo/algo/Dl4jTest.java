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

import org.deeplearning4j.graph.api.Vertex;
import org.deeplearning4j.graph.graph.Graph;
import org.deeplearning4j.graph.models.deepwalk.DeepWalk;
import org.junit.Test;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.util.ArrayList;
import java.util.List;

public class Dl4jTest {
    @Test
    public void blah() {

        DeepWalk.Builder<String, Integer> builder = new DeepWalk.Builder<>();
        builder.vectorSize(10);
        builder.learningRate(0.01);
        builder.windowSize(2);
        DeepWalk<String, Integer> dw = builder.build();

        List<Vertex<String>> nodes = new ArrayList<>();
        nodes.add(new Vertex<>(0, "Mark"));
        nodes.add(new Vertex<>(1, "Michael"));

        Graph<String, Integer> graph = new Graph<>(nodes);
        graph.addEdge(0, 1, -1, false);

        dw.initialize(graph);

        dw.fit(graph, 10);

        for (int i = 0; i < dw.numVertices(); i++) {
            INDArray vertexVector = dw.getVertexVector(i);
            System.out.println("vertexVector = " + vertexVector);
        }

    }
}

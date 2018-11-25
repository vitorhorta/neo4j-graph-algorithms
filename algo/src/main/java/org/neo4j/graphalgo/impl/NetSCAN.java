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
package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public final class NetSCAN extends Algorithm<NetSCAN> {

    public static final String PARTITION_TYPE = "property";
    public static final String WEIGHT_TYPE = "weight";

    private final WeightMapping nodeProperties;
    private final WeightMapping nodeWeights;

    private HeavyGraph graph;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;
    private final int nodeCount;
    private final double eps;
    private final int minPts;
    private final boolean higherBetter;

    private Set<Integer> neighbors = new HashSet<>();
    private boolean[] expanded;
    private boolean[] cores;
    private boolean[] noises;
    private Set<Integer>[] clusters;

    public static class StreamResult {
        public final long nodeId;
        public final long label;

        public StreamResult(long nodeId, long label) {
            this.nodeId = nodeId;
            this.label = label;
            System.out.println(nodeId);
            System.out.println(label);
        }
    }

    public NetSCAN(
            HeavyGraph graph,
            int batchSize,
            int concurrency,
            ExecutorService executor,
            double eps,
            int minPts,
            boolean higherBetter) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;

        this.nodeProperties = this.graph.nodeProperties(PARTITION_TYPE);
        this.nodeWeights = this.graph.nodeProperties(WEIGHT_TYPE);
        this.eps = eps;
        this.minPts = minPts;
        this.higherBetter = higherBetter;
        this.expanded = new boolean[nodeCount];
        this.cores = new boolean[nodeCount];
        this.noises = new boolean[nodeCount];
        this.clusters = new HashSet[nodeCount];
    }


    public Set<Integer>[] clusters() {
        return clusters;
    }

    public NetSCAN compute(Direction direction) {

        Collection<PrimitiveIntIterable> nodes = graph.batchIterables(10000);
        Iterator<PrimitiveIntIterable> iterator = nodes.iterator();
        new ComputeStep(graph, expanded, direction,
                iterator.next(),
                nodeWeights, neighbors, cores, noises, clusters, eps, minPts, higherBetter).run();


        return this;
    }

    @Override
    public NetSCAN me() {
        return this;
    }

    @Override
    public NetSCAN release() {
        graph = null;
        return this;
    }

    private static final class ComputeStep implements Runnable, RelationshipConsumer {

        private final HeavyGraph graph;
        private final boolean[] expanded;
        private boolean[] cores;
        private boolean[] noises;
        private int clusterId = 0;
        private final Set<Integer> neighbors;
        private Set<Integer>[] clusters;

        private final Direction direction;
        private final PrimitiveIntIterable nodes;
        private final double eps;
        private final int minPts;
        private final boolean higherBetter;
        private final WeightMapping nodeWeights;

        private boolean didChange = true;

        private ComputeStep(
                HeavyGraph graph,
                boolean[] expanded,
                Direction direction,
                PrimitiveIntIterable nodes,
                WeightMapping nodeWeights,
                Set<Integer> neighbors,
                boolean[] cores,
                boolean[] noises,
                Set<Integer>[] clusters,
                double eps,
                int minPts,
                boolean higherBetter) {
            this.graph = graph;
            this.expanded = expanded;
            this.direction = direction;
            this.nodes = nodes;
            this.nodeWeights = nodeWeights;
            this.neighbors = neighbors;
            this.cores = cores;
            this.noises = noises;
            this.clusters = clusters;
            this.clusterId = 0;
            this.eps = eps;
            this.minPts = minPts;
            this.higherBetter = higherBetter;
        }

        @Override
        public void run() {
            PrimitiveIntIterator iterator = nodes.iterator();
            while (iterator.hasNext()) {
                compute(iterator.next());
            }
            System.out.println("finish");
        }


        private void compute(int nodeId) {
            if (expanded[nodeId]) return;

            Arrays.fill(expanded, false);
            neighbors.clear();

            graph.forEachRelationship(nodeId, direction, this);
            if (neighbors.size() >= minPts) clusterId++;
            expandCluster(neighbors, nodeId, true);
        }

        private boolean expandCluster(Set<Integer> neighbors, int nodeId, boolean isFirstCore) {
            if (neighbors.size() < minPts) {
                noises[nodeId] = true;
                return false;
            }

            if (isFirstCore) {
                createNewCluster(nodeId, neighbors);
            }

            groupSeedsFromCore(neighbors, nodeId);
            cores[nodeId] = true;
            return true;
        }

        private boolean groupSeedsFromCore(Set<Integer> neighbors, int nodeId) {
            clusters[clusterId].addAll(neighbors);
            Set<Integer> coreNeighbors = neighbors.stream().collect(Collectors.toSet());
            neighbors.clear();
            Iterator it = coreNeighbors.iterator();
            while (it.hasNext()) {
                int neighbor = (int) it.next();
                if (!expanded[neighbor] && !noises[neighbor]) {
                    graph.forEachRelationship(neighbor, direction, this);
                    expandCluster(neighbors, neighbor, false);
                }
            }

            return true;
        }

        private void createNewCluster(int nodeId, Set<Integer> neighbors) {
            if (clusters[clusterId] == null) clusters[clusterId] = new HashSet<>();
            clusters[clusterId].addAll(neighbors);
        }

        @Override
        public boolean accept(
                final int sourceNodeId,
                final int targetNodeId,
                final long relationId) {
            expanded[sourceNodeId] = true;
            double weight = graph.weightOf(sourceNodeId, targetNodeId) * nodeWeights.get(targetNodeId);
            if (weight > eps && higherBetter) neighbors.add(targetNodeId);
            else if (weight < eps && !higherBetter) neighbors.add(targetNodeId);
            return true;
        }

        private void release() {
            // the HPPC release() method allocates new arrays
            // the clear() method overwrite the existing keys with the default value
            // we want to throw away all data to allow for GC collection instead.

        }
    }
}

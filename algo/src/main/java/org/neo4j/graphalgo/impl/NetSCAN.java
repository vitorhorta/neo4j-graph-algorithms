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

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public final class NetSCAN extends Algorithm<NetSCAN> {

    public static final String PARTITION_TYPE = "property";
    public static final String WEIGHT_TYPE = "weight";

    private static final int[] EMPTY_INTS = new int[0];

    private final WeightMapping nodeProperties;
    private final WeightMapping nodeWeights;

    private HeavyGraph graph;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;
    private final int nodeCount;

    private int[] labels;
    private long ranIterations;
    private boolean didConverge;

    public static class StreamResult {
        public final long nodeId;
        public final long label;

        public StreamResult(long nodeId, long label) {
            this.nodeId = nodeId;
            this.label = label;
        }
    }

    public NetSCAN(
            HeavyGraph graph,
            int batchSize,
            int concurrency,
            ExecutorService executor) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;

        this.nodeProperties = this.graph.nodeProperties(PARTITION_TYPE);
        this.nodeWeights = this.graph.nodeProperties(WEIGHT_TYPE);
    }


    public NetSCAN compute(Direction direction) {

        if (labels == null || labels.length != nodeCount) {
            labels = new int[nodeCount];
        }
        ranIterations = 0;
        didConverge = false;

//        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            Collection<PrimitiveIntIterable> nodes = graph.batchIterables(10000);
            Iterator<PrimitiveIntIterable> iterator = nodes.iterator();
            new ComputeStep(graph, labels, Direction.OUTGOING, getProgressLogger(),
                    iterator.next(),
                    nodeWeights).run();

//        }

        return this;
    }

    public long ranIterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    public int[] labels() {
        return labels;
    }

    public IntObjectMap<IntArrayList> groupByPartition() {
        if (labels == null) {
            return null;
        }
        IntObjectMap<IntArrayList> cluster = new IntObjectHashMap<>();

        for (int node = 0, l = labels.length; node < l; node++) {
            int key = labels[node];
            IntArrayList ids = cluster.get(key);
            if (ids == null) {
                ids = new IntArrayList();
                cluster.put(key, ids);
            }
            ids.add(node);
        }

        return cluster;
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

    private static final class InitStep implements Runnable {

        private final HeavyGraph graph;
        private final int[] existingLabels;
        private final Direction direction;
        private final boolean randomizeOrder;
        private final ProgressLogger progressLogger;
        private final PrimitiveIntIterable nodes;
        private final WeightMapping nodeProperties;

        private InitStep(
                HeavyGraph graph,
                int[] existingLabels,
                Direction direction,
                boolean randomizeOrder,
                ProgressLogger progressLogger,
                PrimitiveIntIterable nodes, WeightMapping nodeProperties) {
            this.graph = graph;
            this.existingLabels = existingLabels;
            this.direction = direction;
            this.randomizeOrder = randomizeOrder;
            this.progressLogger = progressLogger;
            this.nodes = nodes;
            this.nodeProperties = nodeProperties;
        }

        @Override
        public void run() {
            initLabels();
        }

        private void initLabels() {
            PrimitiveIntIterator iterator = nodes.iterator();
            while (iterator.hasNext()) {
                int nodeId = iterator.next();
                int existingLabel = (int) this.nodeProperties.get(nodeId, (double) nodeId);
                existingLabels[nodeId] = existingLabel;
            }
        }

        private ComputeStep computeStep(WeightMapping nodeWeights) {
            return new ComputeStep(
                    graph,
                    existingLabels,
                    direction,
                    progressLogger,
                    nodes,
                    nodeWeights);
        }
    }

    private static final class ComputeStep implements Runnable, RelationshipConsumer {

        private final HeavyGraph graph;
        private final int[] existingLabels;
        private final Direction direction;
        private final ProgressLogger progressLogger;
        private final PrimitiveIntIterable nodes;
        private final int maxNode;
        private final IntDoubleHashMap votes;
        private final WeightMapping nodeWeights;

        private boolean didChange = true;
        private long iteration = 0L;

        private ComputeStep(
                HeavyGraph graph,
                int[] existingLabels,
                Direction direction,
                ProgressLogger progressLogger,
                PrimitiveIntIterable nodes,
                WeightMapping nodeWeights) {
            this.graph = graph;
            this.existingLabels = existingLabels;
            this.direction = direction;
            this.progressLogger = progressLogger;
            this.nodes = nodes;
            this.maxNode = (int) (graph.nodeCount() - 1L);
            this.votes = new IntDoubleScatterMap();
            this.nodeWeights = nodeWeights;
        }

        @Override
        public void run() {
            PrimitiveIntIterator iterator = nodes.iterator();
            boolean didChange = false;
            while (iterator.hasNext()) {
                didChange = compute(iterator.next());
            }
            this.didChange = didChange;

        }

        private boolean compute(int nodeId) {
            System.out.println(nodeId);
//            votes.clear();
//            int partition = existingLabels[nodeId];
//            int previous = partition;
//            graph.forEachRelationship(nodeId, direction, this);
//            double weight = Double.NEGATIVE_INFINITY;
//            for (IntDoubleCursor vote : votes) {
//                if (weight < vote.value) {
//                    weight = vote.value;
//                    partition = vote.key;
//                }
//            }
//            progressLogger.logProgress(nodeId, maxNode);
//            if (partition != previous) {
//                existingLabels[nodeId] = partition;
//                return true;
//            }
            return didChange;
        }

        @Override
        public boolean accept(
                final int sourceNodeId,
                final int targetNodeId,
                final long relationId) {
            int partition = existingLabels[targetNodeId];
            double weight = graph.weightOf(sourceNodeId, targetNodeId) * nodeWeights.get(targetNodeId);
            votes.addTo(partition, weight);
            return true;
        }

        private void release() {
            // the HPPC release() method allocates new arrays
            // the clear() method overwrite the existing keys with the default value
            // we want to throw away all data to allow for GC collection instead.

            if (votes.keys != null) {
                votes.keys = EMPTY_INTS;
                votes.clear();
                votes.keys = null;
                votes.values = null;
            }
        }
    }

    private static final class RandomlySwitchingIterable implements PrimitiveIntIterable {
        private final PrimitiveIntIterable delegate;
        private final Random random;

        static PrimitiveIntIterable of(
                boolean randomize,
                PrimitiveIntIterable delegate) {
            return randomize
                    ? new RandomlySwitchingIterable(delegate, ThreadLocalRandom.current())
                    : delegate;
        }

        private RandomlySwitchingIterable(PrimitiveIntIterable delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public PrimitiveIntIterator iterator() {
            return new RandomlySwitchingIterator(delegate.iterator(), random);
        }
    }

    private static final class RandomlySwitchingIterator implements PrimitiveIntIterator {
        private final PrimitiveIntIterator delegate;
        private final Random random;
        private boolean hasSkipped;
        private int skipped;

        private RandomlySwitchingIterator(PrimitiveIntIterator delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public boolean hasNext() {
            return hasSkipped || delegate.hasNext();
        }

        @Override
        public int next() {
            if (hasSkipped) {
                int elem = skipped;
                hasSkipped = false;
                return elem;
            }
            int next = delegate.next();
            if (delegate.hasNext() && random.nextBoolean()) {
                skipped = next;
                hasSkipped = true;
                return delegate.next();
            }
            return next;
        }
    }
}

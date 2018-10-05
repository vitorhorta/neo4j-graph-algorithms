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
package org.neo4j.graphalgo.core.heavyweight;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.MemoryUsage;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.function.IntPredicate;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.LINEAR_SEARCH_LIMIT;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.binarySearch;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.linearSearch;

/**
 * Relation Container built of multiple arrays. The node capacity must be constant and the node IDs have to be
 * smaller then the capacity. The number of relations per node is limited only to the maximum array size of the VM
 * and connections can be added dynamically.
 *
 * @author mknblch
 */
public class AdjacencyMatrix {

    static class Cell {

        private final int[] nodeIds;
        private final double[] weights;

        public Cell(int[] nodeIds, double[] weights) {

            this.nodeIds = nodeIds;
            this.weights = weights;
        }

        public int[] nodeIds() {
            return nodeIds;
        }

        public double[] weights() {
            return weights;
        }
    }

    private static final int[] EMPTY_INTS = new int[0];
    private static final double[] EMPTY_DOUBLES = new double[0];

    /**
     * mapping from nodeId to outgoing degree
     */
    private final int[] outOffsets;
    /**
     * mapping from nodeId to incoming degree
     */
    private final int[] inOffsets;
    /**
     * matrix nodeId x [outgoing edge-relationIds..]
     */
    private final int[][] outgoing;
    /**
     * matrix nodeId x [incoming edge-relationIds..]
     */
    private final int[][] incoming;


    /**
     * matrix nodeId x [outgoing edge-relationIds..]
     */
    private final double[][] outgoingWeights;
    /**
     * matrix nodeId x [incoming edge-relationIds..]
     */
    private final double[][] incomingWeights;

    private boolean sorted = false;

    private final AllocationTracker tracker;

    public AdjacencyMatrix(int nodeCount, boolean sorted, AllocationTracker tracker) {
        this(nodeCount, true, true, sorted, tracker);
    }

    AdjacencyMatrix(
            int nodeCount,
            boolean withIncoming,
            boolean withOutgoing,
            boolean sorted,
            AllocationTracker tracker) {
        this(nodeCount, withIncoming, withOutgoing, sorted, true, tracker);
    }

    AdjacencyMatrix(
            int nodeCount,
            boolean withIncoming,
            boolean withOutgoing,
            boolean sorted,
            boolean preFill,
            AllocationTracker tracker) {
        if (withOutgoing) {
            tracker.add(MemoryUsage.sizeOfIntArray(nodeCount));
            tracker.add(MemoryUsage.sizeOfObjectArray(nodeCount));
            this.outOffsets = new int[nodeCount];
            this.outgoing = new int[nodeCount][];
            this.outgoingWeights = new double[nodeCount][];
            if (preFill) {
                Arrays.fill(outgoing, EMPTY_INTS);
                Arrays.fill(outgoingWeights, EMPTY_DOUBLES);
            }
        } else {
            this.outOffsets = null;
            this.outgoing = null;
            this.outgoingWeights = null;
        }
        if (withIncoming) {
            tracker.add(MemoryUsage.sizeOfIntArray(nodeCount));
            tracker.add(MemoryUsage.sizeOfObjectArray(nodeCount));
            this.inOffsets = new int[nodeCount];
            this.incoming = new int[nodeCount][];
            this.incomingWeights = new double[nodeCount][];
            if (preFill) {
                Arrays.fill(incoming, EMPTY_INTS);
                Arrays.fill(incomingWeights, EMPTY_DOUBLES);
            }
        } else {
            this.inOffsets = null;
            this.incoming = null;
            this.incomingWeights = null;
        }
        this.sorted = sorted;
        this.tracker = tracker;
    }

    /**
     * initialize array for outgoing connections
     */
    public int[] armOut(int sourceNodeId, int degree) {
        if (degree > 0) {
            tracker.add(MemoryUsage.sizeOfIntArray(degree));
            outgoing[sourceNodeId] = new int[degree];
        }
        return outgoing[sourceNodeId];
    }

    public Cell armOutCell(int sourceNodeId, int degree) {
        if (degree > 0) {
            tracker.add(MemoryUsage.sizeOfIntArray(degree));
            outgoing[sourceNodeId] = new int[degree];
            outgoingWeights[sourceNodeId] = new double[degree];
        }
        return new Cell(outgoing[sourceNodeId], outgoingWeights[sourceNodeId]);
    }

    /**
     * initialize array for incoming connections
     */
    public int[] armIn(int targetNodeId, int degree) {
        if (degree > 0) {
            tracker.add(MemoryUsage.sizeOfIntArray(degree));
            incoming[targetNodeId] = new int[degree];
        }
        return incoming[targetNodeId];
    }

    public Cell armInCell(int targetNodeId, int degree) {
        if (degree > 0) {
            tracker.add(MemoryUsage.sizeOfIntArray(degree));
            incoming[targetNodeId] = new int[degree];
            incomingWeights[targetNodeId] = new double[degree];
        }
        return new Cell(incoming[targetNodeId], incomingWeights[targetNodeId]);
    }


    void setOutDegree(int nodeId, final int degree) {
        outOffsets[nodeId] = degree;
    }

    void setInDegree(int nodeId, final int degree) {
        inOffsets[nodeId] = degree;
    }

    /**
     * grow array for outgoing connections
     */
    private void growOut(int sourceNodeId, int length) {
        assert length >= 0 : "size must be positive (got " + length + "): likely integer overflow?";
        if (outgoing[sourceNodeId].length < length) {
            outgoing[sourceNodeId] = growArray(outgoing[sourceNodeId], length);
            outgoingWeights[sourceNodeId] = growArray(outgoingWeights[sourceNodeId], length);
        }
    }

    /**
     * grow array for incoming connections
     */
    private void growIn(int targetNodeId, int length) {
        assert length >= 0 : "size must be positive (got " + length + "): likely integer overflow?";
        if (incoming[targetNodeId].length < length) {
            incoming[targetNodeId] = growArray(incoming[targetNodeId], length);
            outgoingWeights[targetNodeId] = growArray(outgoingWeights[targetNodeId], length);
        }
    }

    private int[] growArray(int[] array, int length) {
        int newSize = ArrayUtil.oversize(length, RamUsageEstimator.NUM_BYTES_INT);
        tracker.remove(MemoryUsage.sizeOfIntArray(array.length));
        tracker.add(MemoryUsage.sizeOfIntArray(newSize));
        return Arrays.copyOf(array, newSize);
    }

    private double[] growArray(double[] array, int length) {
        int newSize = ArrayUtil.oversize(length, RamUsageEstimator.NUM_BYTES_INT);
        tracker.remove(MemoryUsage.sizeOfIntArray(array.length));
        tracker.add(MemoryUsage.sizeOfIntArray(newSize));
        return Arrays.copyOf(array, newSize);
    }

    /**
     * add outgoing relation
     */
    public void addOutgoing(int sourceNodeId, int targetNodeId) {
        final int degree = outOffsets[sourceNodeId];
        final int nextDegree = degree + 1;
        growOut(sourceNodeId, nextDegree);
        outgoing[sourceNodeId][degree] = targetNodeId;
        outgoingWeights[sourceNodeId][degree] = 0;
        outOffsets[sourceNodeId] = nextDegree;
    }

    public void addOutgoing(int sourceNodeId, int targetNodeId, double weight) {
        final int degree = outOffsets[sourceNodeId];
        final int nextDegree = degree + 1;
        growOut(sourceNodeId, nextDegree);
        outgoing[sourceNodeId][degree] = targetNodeId;
        outgoingWeights[sourceNodeId][degree] = weight;
        outOffsets[sourceNodeId] = nextDegree;
    }

    /**
     * checks for outgoing target node
     */
    public boolean hasOutgoing(int sourceNodeId, int targetNodeId) {

        final int degree = outOffsets[sourceNodeId];
        final int[] rels = outgoing[sourceNodeId];

        if (sorted && degree > LINEAR_SEARCH_LIMIT) {
            return binarySearch(rels, degree, targetNodeId);
        }

        return linearSearch(rels, degree, targetNodeId);
    }

    public int getTargetOutgoing(int nodeId, int index) {
        final int degree = outOffsets[nodeId];
        if (index < 0 || index >= degree) {
            return -1;
        }
        return outgoing[nodeId][index];
    }

    public int getTargetIncoming(int nodeId, int index) {
        final int degree = inOffsets[nodeId];
        if (index < 0 || index >= degree) {
            return -1;
        }
        return incoming[nodeId][index];
    }

    public int getTargetBoth(int nodeId, int index) {
        final int outDegree = outOffsets[nodeId];
        if (index >= 0 && index < outDegree) {
            return outgoing[nodeId][index];
        } else {
            index -= outDegree;
            final int inDegree = inOffsets[nodeId];
            if (index >= 0 && index < inDegree) {
                return incoming[nodeId][index];
            }
        }
        return -1;
    }


    /**
     * checks for incoming target node
     */
    public boolean hasIncoming(int sourceNodeId, int targetNodeId) {

        final int degree = inOffsets[sourceNodeId];
        final int[] rels = incoming[sourceNodeId];

        if (sorted && degree > LINEAR_SEARCH_LIMIT) {
            return binarySearch(rels, degree, targetNodeId);
        }

        return linearSearch(rels, degree, targetNodeId);
    }

    /**
     * add incoming relation
     */
    public void addIncoming(int sourceNodeId, int targetNodeId) {
        final int degree = inOffsets[targetNodeId];
        final int nextDegree = degree + 1;
        growIn(targetNodeId, nextDegree);
        incoming[targetNodeId][degree] = sourceNodeId;
        inOffsets[targetNodeId] = nextDegree;
    }

    /**
     * get the degree for node / direction
     *
     * @throws NullPointerException if the direction hasn't been loaded.
     */
    public int degree(int nodeId, Direction direction) {
        switch (direction) {
            case OUTGOING: {
                return outOffsets[nodeId];
            }
            case INCOMING: {
                return inOffsets[nodeId];
            }
            default: {
                return inOffsets[nodeId] + outOffsets[nodeId];
            }
        }
    }

    /**
     * iterate over each edge at the given node using an unweighted consumer
     */
    public void forEach(int nodeId, Direction direction, RelationshipConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                break;
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                break;
            default:
                forEachIncoming(nodeId, consumer);
                forEachOutgoing(nodeId, consumer);
                break;
        }
    }

    /**
     * iterate over each edge at the given node using a weighted consumer
     */
    public void forEach(int nodeId, Direction direction, WeightMapping weights, WeightedRelationshipConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(nodeId, weights, consumer);
                break;
            case INCOMING:
                forEachIncoming(nodeId, weights, consumer);
                break;
            default:
                forEachOutgoing(nodeId, weights, consumer);
                forEachIncoming(nodeId, weights, consumer);
                break;
        }
    }

    public int capacity() {
        return outOffsets != null
                ? outOffsets.length
                : inOffsets != null
                ? inOffsets.length
                : 0;
    }

    public void addMatrix(AdjacencyMatrix other, int offset, int length) {
        if (other.outgoing != null) {
            System.arraycopy(other.outgoing, 0, outgoing, offset, length);
            System.arraycopy(other.outOffsets, 0, outOffsets, offset, length);
        }
        if (other.incoming != null) {
            System.arraycopy(other.incoming, 0, incoming, offset, length);
            System.arraycopy(other.inOffsets, 0, inOffsets, offset, length);
        }
    }

    private void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, outs[i], RawValues.combineIntInt(nodeId, outs[i]));
        }
    }

    private void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] ins = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, ins[i], RawValues.combineIntInt(ins[i], nodeId));
        }
    }

    private void forEachOutgoing(int nodeId, WeightMapping weights, WeightedRelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        double[] outWeights = outgoingWeights[nodeId];
        for (int i = 0; i < degree; i++) {
//            final long relationId = RawValues.combineIntInt(nodeId, outs[i]);
            consumer.accept(nodeId, outs[i], -1L, outWeights[i]);
        }
    }

    private void forEachIncoming(int nodeId, WeightMapping weights, WeightedRelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] neighbours = incoming[nodeId];
        double[] neighbourWeights = incomingWeights[nodeId];
        for (int i = 0; i < degree; i++) {
//            final long relationId = RawValues.combineIntInt(neighbours[i], nodeId);
            consumer.accept(nodeId, neighbours[i], -1L, neighbourWeights[i]);
        }
    }

    public NodeIterator nodesWithRelationships(Direction direction) {
        if (direction == Direction.OUTGOING) {
            return new DegreeCheckingNodeIterator(outOffsets);
        } else {
            return new DegreeCheckingNodeIterator(inOffsets);
        }
    }

    public void sortIncoming(int node) {
        Arrays.sort(incoming[node], 0, inOffsets[node]);
    }

    public void sortOutgoing(int node) {
        Arrays.sort(outgoing[node], 0, outOffsets[node]);
    }

    public void sortAll(ExecutorService pool, int concurrency) {
        ParallelUtil.iterateParallel(pool, outgoing.length, concurrency, node -> {
            sortIncoming(node);
            sortOutgoing(node);
        });
        sorted = true;
    }

    public void intersectAll(int nodeA, IntersectionConsumer consumer) {
        int outDegreeA = outOffsets[nodeA];
        int[] neighboursA = outgoing[nodeA];
        for (int i = 0; i < outDegreeA; i++) {
            int nodeB = neighboursA[i];
            int outDegreeB = outOffsets[nodeB];
            int[] neighboursB = outgoing[nodeB];
            int[] jointNeighbours = Intersections.getIntersection(neighboursA, outDegreeA, neighboursB, outDegreeB);
            for (int nodeC : jointNeighbours) {
                if (nodeB < nodeC) consumer.accept(nodeA,nodeB,nodeC);
            }
        }
    }

    private static class DegreeCheckingNodeIterator implements NodeIterator {

        private final int[] array;

        DegreeCheckingNodeIterator(int[] array) {
            this.array = array != null ? array : EMPTY_INTS;
        }

        @Override
        public void forEachNode(IntPredicate consumer) {
            for (int node = 0; node < array.length; node++) {
                if (array[node] > 0 && !consumer.test(node)) {
                    break;
                }
            }
        }

        @Override
        public PrimitiveIntIterator nodeIterator() {
            return new PrimitiveIntIterator() {
                int index = findNext();

                @Override
                public boolean hasNext() {
                    return index < array.length;
                }

                @Override
                public int next() {
                    try {
                        return index;
                    } finally {
                        index = findNext();
                    }
                }

                private int findNext() {
                    int length = array.length;
                    for (int n = index + 1; n < length; n++) {
                        if (array[n] > 0) {
                            return n;
                        }
                    }
                    return length;
                }
            };
        }
    }
}

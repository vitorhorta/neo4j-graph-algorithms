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
package org.neo4j.graphalgo.impl.betweenness;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.Similarity;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class ParallelNearestNeighbors extends Algorithm<ParallelNearestNeighbors> {

    // the graph
    private Graph graph;
    // AI counts up for every node until nodeCount is reached
    private volatile AtomicInteger nodeQueue = new AtomicInteger();
    // atomic double array which supports only atomic-add
    private AtomicDoubleArray distance;
    private AtomicIntegerArray closest;
    // the node count
    private final int nodeCount;
    // global executor service
    private final ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;
    private Direction direction = Direction.OUTGOING;
    private double divisor = 1.0;

    /**
     * constructs a parallel centrality solver
     *
     * @param graph           the graph iface
     * @param executorService the executor service
     * @param concurrency     desired number of threads to spawn
     */
    public ParallelNearestNeighbors(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.distance = new AtomicDoubleArray(nodeCount);
        this.closest = new AtomicIntegerArray(nodeCount);
    }

    /**
     * compute centrality
     *
     * @return itself for method chaining
     */
    public ParallelNearestNeighbors compute() {
        nodeQueue.set(0);
        final ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            futures.add(executorService.submit(new NNTask()));
        }
        ParallelUtil.awaitTermination(futures);
        return this;
    }

    /**
     * get the centrality array
     *
     * @return array with centrality
     */
    public AtomicDoubleArray getDistance() {
        return distance;
    }

    /**
     * emit the result stream
     *
     * @return stream if Results
     */
    public Stream<ParallelNearestNeighbors.Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId ->
                        new ParallelNearestNeighbors.Result(
                                graph.toOriginalNodeId(nodeId),
                                graph.toOriginalNodeId(closest.get(nodeId)),
                                distance.get(nodeId)));
    }

    @Override
    public ParallelNearestNeighbors me() {
        return this;
    }

    @Override
    public ParallelNearestNeighbors release() {
        graph = null;
        distance = null;
        return null;
    }

    /**
     * a NNTask takes one element from the nodeQueue as long as
     * it is lower then nodeCount and calculates it's centrality
     */
    private class NNTask implements Runnable {

        private final IntStack stack;
        private final IntArrayDeque queue;

        private final double[] localDistance;
        private final int[] localClosest;
        private final Similarity similarity;

        private NNTask() {
            this.stack = new IntStack();
            this.queue = new IntArrayDeque();
            this.localClosest = new int[nodeCount];
            Arrays.fill(localClosest, -1);
            this.localDistance = new double[nodeCount];
            Arrays.fill(localDistance, Double.MAX_VALUE);
            this.similarity = new Similarity();
        }

        @Override
        public void run() {
            for (; ; ) {
                reset();
                final int startNodeId = nodeQueue.getAndIncrement();
                if (startNodeId >= nodeCount || !running()) {
                    return;
                }
                getProgressLogger().logProgress((double) startNodeId / (nodeCount - 1));
                queue.addLast(startNodeId);
                while (!queue.isEmpty()) {
                    int node = queue.removeFirst();
                    stack.push(node);

                    graph.forEachNode((otherNodeId) -> {
                        if (node == otherNodeId) {
                            return true;
                        }

                        double currentDistance = this.localDistance[node];
                        double distance = similarity.euclideanDistance(
                                graph.vectorOf(graph.toOriginalNodeId(node)),
                                graph.vectorOf(graph.toOriginalNodeId(otherNodeId)));

                        if (distance < currentDistance) {
                            this.localDistance[node] = distance;
                            this.localClosest[node] = otherNodeId;
                        }

                        return true;
                    });

                    distance.add(node, this.localDistance[node]);
                    closest.set(node, this.localClosest[node]);
                }

            }
        }

        /**
         * reset local state
         */
        private void reset() {
            stack.clear();
            queue.clear();
            Arrays.fill(localClosest, -1);
            Arrays.fill(localDistance, Double.MAX_VALUE);
        }
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final long nodeId;
        public final long closestNodeId;
        public final double distance;

        public Result(long nodeId, long closestNodeId, double distance) {
            this.nodeId = nodeId;
            this.closestNodeId = closestNodeId;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", closestNodeId=" + closestNodeId +
                    ", distance=" + distance +
                    '}';
        }
    }
}

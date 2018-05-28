package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Modularity based clustering algorithm. Uses {@link ModularityOptimization} algorithm
 * to perform optimization. if a better community structure has been found it is taken as
 * input for the next round. the algorithm creates a virtual graph based on the previous
 * community structure.
 *
 * @author mknblch
 */
public class Louvain implements LouvainAlgorithm {

    private final int maxIterations;
    private final int rootNodeCount;
    private int iterations;
    private final int innerMaxIterations;
    private final ExecutorService pool;
    private final int concurrency;
    private final AllocationTracker tracker;
    private ProgressLogger progressLogger;
    private TerminationFlag terminationFlag;
    private int[] communities;
    private Graph root;
    private int communityCount = 0;

    public Louvain(Graph graph,
                   int maxIterations,
                   int innerMaxIterations,
                   ExecutorService pool,
                   int concurrency,
                   AllocationTracker tracker) {
        this.root = graph;
        this.maxIterations = maxIterations;
        this.innerMaxIterations = innerMaxIterations;
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        rootNodeCount = Math.toIntExact(graph.nodeCount());
        communities = new int[rootNodeCount];
        communityCount = rootNodeCount;
        Arrays.setAll(communities, i -> i);
    }

    @Override
    public LouvainAlgorithm compute(int maxIterations) {
        // temporary graph
        Graph graph = this.root;
        // current graph modularity worst possible value
        double q = -Double.MAX_VALUE;

        for (iterations = 0; iterations < this.maxIterations; iterations++) {
            // start louvain
            final ModularityOptimization modularityOptimization = new ModularityOptimization(graph, innerMaxIterations, pool, concurrency, tracker)
                    .withProgressLogger(progressLogger)
                    .withTerminationFlag(terminationFlag)
                    .compute(maxIterations);
            // compare new modularity
            if (modularityOptimization.getModularity() > q) {
                // save given ids
                // modularity increased
                q = modularityOptimization.getModularity();
                // rebuild graph based on the community structure
                graph = rebuild(graph, modularityOptimization.getCommunityIds());
                // release the old algo instance
                modularityOptimization.release();
            } else {
                // its worse, stop
                modularityOptimization.release();
                break;
            }
        }

        double finalQ = q;
        progressLogger.logDone(() -> "final modularity " + finalQ + " after " + (iterations + 1) + " iterations");

        return this;
    }

    private Graph rebuild(Graph graph, int[] communityIds) {

        // count and normalize community structure
        communityCount = ModularityOptimization.normalize(communityIds);
        final int nodeCount = communityIds.length;
        // bag of nodeId->{nodeId, ..}
        final IntObjectMap<IntScatterSet> relationships = new IntObjectScatterMap<>(nodeCount);
        // accumulated weights
        final LongDoubleScatterMap weights = new LongDoubleScatterMap(nodeCount);
        // for each node in the current graph
        for (int i = 0; i < nodeCount; i++) {
            // map node id to community id
            final int source = communityIds[i];
            // get degree 1 transitions from current node
            graph.forEachRelationship(i, Direction.OUTGOING, (s, t, r) -> {
                // mapping
                final int target = communityIds[t];
                // omit self loops
                if (source == target) {
                    return true;
                }
                // add IN and OUT relation
                find(relationships, source).add(target);
                find(relationships, target).add(source);
                // aggregate weights
                final double value = graph.weightOf(s, t);
                weights.addTo(RawValues.combineIntInt(source, target), value);
                weights.addTo(RawValues.combineIntInt(target, source), value);
                return true;
            });
        }
        // rebuild community array
        final int[] ints = new int[rootNodeCount];
        Arrays.setAll(ints, i -> communityIds[communities[i]]);
        communities = ints;
        // create temporary graph
        return new LouvainGraph(communityCount, relationships, weights);
    }

    @Override
    public int[] getCommunityIds() {
        return communities;
    }

    @Override
    public int getIterations() {
        return iterations;
    }

    @Override
    public long getCommunityCount() {
        return communityCount;
    }

    @Override
    public Stream<Result> resultStream() {
        return IntStream.range(0, rootNodeCount)
                .mapToObj(i -> new Result(i, communities[i]));
    }

    @Override
    public LouvainAlgorithm withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }

    @Override
    public LouvainAlgorithm withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    private static IntScatterSet find(IntObjectMap<IntScatterSet> relationships, int n) {
        final IntScatterSet intCursors = relationships.get(n);
        if (null == intCursors) {
            final IntScatterSet newList = new IntScatterSet();
            relationships.put(n, newList);
            return newList;
        }
        return intCursors;
    }
}

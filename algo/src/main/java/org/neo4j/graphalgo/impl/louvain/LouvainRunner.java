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
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class LouvainRunner implements LouvainAlgorithm {

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

    private Graph graph;
    private Louvain louvain;
    private int communityCount = 0;

    public LouvainRunner(Graph graph, int maxIterations, int innerMaxIterations, ExecutorService pool, int concurrency, AllocationTracker tracker) {
        this.graph = graph;
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
    public LouvainAlgorithm compute() {

        double q = Double.NEGATIVE_INFINITY;
        for (iterations = 0; iterations < maxIterations; iterations++) {
            louvain = new Louvain(graph, innerMaxIterations, pool, concurrency, tracker)
                    .withProgressLogger(progressLogger)
                    .withTerminationFlag(terminationFlag)
                    .compute();
            if (louvain.getModularity() > q) {
                q = louvain.getModularity();
                rebuild();
            } else {
                break;
            }
        }

        return this;
    }

    private void rebuild() {

        progressLogger.logDone(() -> "rebuilding graph");
        final int nodeCount = Math.toIntExact(graph.nodeCount());
        final int[] communityIds = louvain.getCommunityIds();
        communityCount = Louvain.normalize(communityIds);
        final IntObjectMap<IntScatterSet> relationships = new IntObjectScatterMap<>(communityCount);
        final LongDoubleScatterMap weights = new LongDoubleScatterMap(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            final int source = communityIds[i];
            graph.forEachRelationship(i, Direction.OUTGOING, (s, t, r) -> {
                final int target = communityIds[t];
                find(relationships, source).add(target);
                weights.addTo(RawValues.combineIntInt(source, target), graph.weightOf(s, t));
                return true;
            });
        }

        final int[] ints = new int[rootNodeCount];
        Arrays.setAll(ints, i -> communityIds[communities[i]]);
        communities = ints;
        graph = new LouvainGraph(communityCount, relationships, weights);
    }

    @Override
    public int[] getCommunityIds() {
        return louvain.getCommunityIds();
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
        return null;
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

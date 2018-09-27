package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class WeightedComputeStep implements ComputeStep {
    private static final int S_INIT = 0;
    private static final int S_CALC = 1;
    private static final int S_SYNC = 2;

    private int state;

    private int[] starts;
    private int[] lengths;
    private int[] sourceNodeIds;
    private final RelationshipIterator relationshipIterator;
    private final Degrees degrees;

    private final double alpha;
    private final double dampingFactor;

    private double[] pageRank;
    private double[] deltas;
    private int[][] nextScores;
    private int[][] prevScores;

    private final RelationshipWeights relationshipWeights;
    private final int partitionSize;
    private final int startNode;
    private final int endNode;

    WeightedComputeStep(
            double dampingFactor,
            int[] sourceNodeIds,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            RelationshipWeights relationshipWeights,
            int partitionSize,
            int startNode) {
        this.dampingFactor = dampingFactor;
        this.alpha = 1.0 - dampingFactor;
        this.sourceNodeIds = sourceNodeIds;
        this.relationshipIterator = relationshipIterator;
        this.degrees = degrees;
        this.relationshipWeights = relationshipWeights;
        this.partitionSize = partitionSize;
        this.startNode = startNode;
        this.endNode = startNode + partitionSize;
        state = S_INIT;
    }

    public void setStarts(int starts[], int[] lengths) {
        this.starts = starts;
        this.lengths = lengths;
    }

    @Override
    public void run() {
        if (state == S_CALC) {
            singleIteration();
            state = S_SYNC;
        } else if (state == S_SYNC) {
            synchronizeScores(combineScores());
            state = S_CALC;
        } else if (state == S_INIT) {
            initialize();
            state = S_CALC;
        }
    }

    private void initialize() {
        this.nextScores = new int[starts.length][];
        Arrays.setAll(nextScores, i -> new int[lengths[i]]);

        double[] partitionRank = new double[partitionSize];

        if(sourceNodeIds.length == 0) {
            Arrays.fill(partitionRank, alpha);
        } else {
            Arrays.fill(partitionRank,0);

            int[] partitionSourceNodeIds = IntStream.of(sourceNodeIds)
                    .filter(sourceNodeId -> sourceNodeId >= startNode && sourceNodeId < endNode)
                    .toArray();

            for (int sourceNodeId : partitionSourceNodeIds) {
                partitionRank[sourceNodeId - this.startNode] = alpha;
            }
        }


        this.pageRank = partitionRank;
        this.deltas = Arrays.copyOf(partitionRank, partitionSize);
    }

    private void singleIteration() {
        int startNode = this.startNode;
        int endNode = this.endNode;
        RelationshipIterator rels = this.relationshipIterator;
        for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[nodeId - startNode];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    double[] tempSumOfWeights = new double[1];
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        tempSumOfWeights[0] += relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                        return true;
                    });

                    double sumOfWeights = tempSumOfWeights[0];

                    rels.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        double proportion = relationshipWeights.weightOf(sourceNodeId, targetNodeId) / sumOfWeights;

                        int srcRankDelta = (int) (100_000 * (delta * proportion));
                        if (srcRankDelta != 0) {
                            int idx = binaryLookup(targetNodeId, starts);
                            nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
                        }
                        return true;
                    });
                }
            }
        }
    }

    public void prepareNextIteration(int[][] prevScores) {
        this.prevScores = prevScores;
    }

    private int[] combineScores() {
        assert prevScores != null;
        assert prevScores.length >= 1;
        int[][] prevScores = this.prevScores;

        int length = prevScores.length;
        int[] allScores = prevScores[0];
        for (int i = 1; i < length; i++) {
            int[] scores = prevScores[i];
            for (int j = 0; j < scores.length; j++) {
                allScores[j] += scores[j];
                scores[j] = 0;
            }
        }

        return allScores;
    }

    private void synchronizeScores(int[] allScores) {
        double dampingFactor = this.dampingFactor;
        double[] pageRank = this.pageRank;

        int length = allScores.length;
        for (int i = 0; i < length; i++) {
            int sum = allScores[i];

            double delta = dampingFactor * (sum / 100_000.0);
            pageRank[i] += delta;
            deltas[i] = delta;
            allScores[i] = 0;
        }
    }

    @Override
    public int[][] nextScores() {
        return nextScores;
    }

    @Override
    public double[] pageRank() {
        return pageRank;
    }

    @Override
    public int[] starts() {
        return starts;
    }

}

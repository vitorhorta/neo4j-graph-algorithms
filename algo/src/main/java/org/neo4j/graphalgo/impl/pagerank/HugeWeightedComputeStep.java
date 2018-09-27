package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.HugeDegrees;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.api.HugeRelationshipWeights;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public class HugeWeightedComputeStep extends HugeBaseComputeStep {
    HugeWeightedComputeStep(
            double dampingFactor,
            long[] sourceNodeIds,
            HugeRelationshipIterator relationshipIterator,
            HugeDegrees degrees,
            HugeRelationshipWeights relationshipWeights,
            AllocationTracker tracker,
            int partitionSize,
            long startNode) {
        super(dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                relationshipWeights,
                tracker,
                partitionSize,
                startNode);
    }

    void singleIteration() {
        long startNode = this.startNode;
        long endNode = this.endNode;
        HugeRelationshipIterator rels = this.relationshipIterator;
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[(int) (nodeId - startNode)];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    double[] tempSumOfWeights = new double[1];
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                        tempSumOfWeights[0] += relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                        return true;
                    });

                    double sumOfWeights = tempSumOfWeights[0];

                    rels.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                        double proportion = relationshipWeights.weightOf(sourceNodeId, targetNodeId) / sumOfWeights;

                        int srcRankDelta = (int) (100_000 * (delta * proportion));
                        if (srcRankDelta != 0) {
                            int idx = binaryLookup(targetNodeId, starts);
                            nextScores[idx][(int) (targetNodeId - starts[idx])] += srcRankDelta;
                        }
                        return true;
                    });
                }
            }
        }
    }
}

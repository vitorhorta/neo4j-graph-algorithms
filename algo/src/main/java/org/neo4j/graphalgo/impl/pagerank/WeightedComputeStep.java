package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class WeightedComputeStep extends BaseComputeStep {
    WeightedComputeStep(
            double dampingFactor,
            int[] sourceNodeIds,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            RelationshipWeights relationshipWeights,
            int partitionSize,
            int startNode) {
        super(dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                relationshipWeights,
                partitionSize,
                startNode);
    }

    void singleIteration() {
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

}

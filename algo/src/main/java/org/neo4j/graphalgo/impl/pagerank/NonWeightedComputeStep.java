package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class NonWeightedComputeStep extends BaseComputeStep implements WeightedRelationshipConsumer {


    NonWeightedComputeStep(
            double dampingFactor,
            int[] sourceNodeIds,
            WeightedRelationshipIterator relationshipIterator,
            Degrees degrees,
            int partitionSize,
            int startNode) {
        super(dampingFactor, sourceNodeIds, relationshipIterator, degrees, partitionSize, startNode);
    }


    private int srcRankDelta;


    void singleIteration() {
        int startNode = this.startNode;
        int endNode = this.endNode;
        WeightedRelationshipIterator rels = this.relationshipIterator;
        for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[nodeId - startNode];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    srcRankDelta = (int) (100_000 * (delta / degree));
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId, double weight) {
        if (srcRankDelta != 0) {
            int idx = binaryLookup(targetNodeId, starts);
            nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
        }
        return true;
    }
}

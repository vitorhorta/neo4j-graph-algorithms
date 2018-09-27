package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;

public class NonWeightedComputeStepFactory implements  ComputeStepFactory {
    public ComputeStep createComputeStep(double dampingFactor, int[] sourceNodeIds, RelationshipIterator relationshipIterator, Degrees degrees, RelationshipWeights relationshipWeights, int partitionCount, int start) {
        return new ComputeStep(
                dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                relationshipWeights,
                partitionCount,
                start
        );
    }
}

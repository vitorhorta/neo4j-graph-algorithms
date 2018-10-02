package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;


public interface PageRankVariant {
    ComputeStep createComputeStep(double dampingFactor, int[] sourceNodeIds, WeightedRelationshipIterator relationshipIterator, Degrees degrees, RelationshipWeights relationshipWeights, int partitionCount, int start, double[] aggregatedDegrees);

    HugeComputeStep createHugeComputeStep(double dampingFactor, long[] sourceNodeIds, HugeRelationshipIterator relationshipIterator, HugeDegrees degrees, HugeRelationshipWeights relationshipWeights, AllocationTracker tracker, int partitionCount, long start, double[] aggregatedDegrees);

    DegreeComputer degreeComputer(Graph graph);

}



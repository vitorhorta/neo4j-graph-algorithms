package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class LouvainGraph implements Graph {

    private final IntLongMap mapping;
    private final int nodeCount;
    private final IntObjectMap<? extends IntContainer> graph;
    private final LongDoubleMap weights;

    LouvainGraph(int newNodeCount, IntObjectMap<? extends IntContainer> graph, IntLongMap mapping, LongDoubleMap weights) {
        this.mapping = mapping;
        this.nodeCount = newNodeCount;
        this.graph = graph;
        this.weights = weights;
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        return -1;
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return mapping.getOrDefault(nodeId, -1);
    }

    @Override
    public boolean contains(long nodeId) {
        return false;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer) {
        final IntContainer intCursors = graph.get(nodeId);
        if (null == intCursors) {
            return;
        }
        intCursors.forEach((IntProcedure) t -> consumer.accept(nodeId, t, -1));
    }

    @Override
    public double weightOf(int sourceNodeId, int targetNodeId) {
        return weights.getOrDefault(RawValues.combineIntInt(sourceNodeId, targetNodeId), 0);
    }

    @Override
    public String getType() {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void canRelease(boolean canRelease) {
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public boolean exists(int sourceNodeId, int targetNodeId, Direction direction) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        throw new IllegalStateException("not implemented");
    }



}

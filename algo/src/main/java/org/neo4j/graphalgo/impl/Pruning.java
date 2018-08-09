package org.neo4j.graphalgo.impl;


import org.apache.commons.lang3.ArrayUtils;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Pruning {

    private final double lambda;
    private final ProgressLogger progressLogger;
    private ExecutorService executor;
    private int concurrency;
    private int batchSize;

    public Pruning() {
        this(0.7, ProgressLogger.NULL_LOGGER, Pools.DEFAULT, 1, 10);
    }

    public Pruning(double lambda) {
        this(lambda, ProgressLogger.NULL_LOGGER, Pools.DEFAULT, 1, 10);
    }

    public Pruning(double lambda, ProgressLogger progressLogger, ExecutorService executor, int concurrency, int batchSize) {

        this.lambda = lambda;
        this.progressLogger = progressLogger;
        this.executor = executor;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
    }

    public Embedding prune(Embedding prevEmbedding, Embedding embedding) {

        INDArray embeddingToPrune = Nd4j.hstack(prevEmbedding.getNDEmbedding(), embedding.getNDEmbedding());
        Feature[] featuresToPrune = ArrayUtils.addAll(prevEmbedding.getFeatures(), embedding.getFeatures());


        progressLogger.log("Feature Pruning: Creating features graph");
        final Graph graph = loadFeaturesGraph(embeddingToPrune, prevEmbedding.features.length);
        progressLogger.log("Feature Pruning: Created features graph");

        progressLogger.log("Feature Pruning: Finding features to keep");
        int[] featureIdsToKeep = findConnectedComponents(graph)
                .collect(Collectors.groupingBy(item -> item.setId))
                .values()
                .stream()
                .mapToInt(results -> results.stream().mapToInt(value -> (int) value.nodeId).min().getAsInt())
                .toArray();
        progressLogger.log("Feature Pruning: Found features to keep");

        progressLogger.log("Feature Pruning: Pruning embeddings");
        INDArray prunedNDEmbedding = pruneEmbedding(embeddingToPrune, featureIdsToKeep);
        progressLogger.log("Feature Pruning: Pruned embeddings");


        Feature[] prunedFeatures = new Feature[featureIdsToKeep.length];

        for (int index = 0; index < featureIdsToKeep.length; index++) {
            prunedFeatures[index] = featuresToPrune[featureIdsToKeep[index]];
        }


        return new Embedding(prunedFeatures, prunedNDEmbedding);
    }

    private Stream<DisjointSetStruct.Result> findConnectedComponents(Graph graph) {
        GraphUnionFind algo = new GraphUnionFind(graph);
        DisjointSetStruct struct = algo.compute(lambda);
        algo.release();
        DSSResult dssResult = new DSSResult(struct);
        return dssResult.resultStream(graph);
    }

    private Graph loadFeaturesGraph(INDArray embedding, int numPrevFeatures) {
        int nodeCount = embedding.columns();

        progressLogger.log("Creating IdMap");
        IdMap idMap = new IdMap(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            idMap.add(i);
        }
        idMap.buildMappedIds();
        progressLogger.log("Created IdMap");

        WeightMap relWeights = new WeightMap(nodeCount, 0, -1);
        AdjacencyMatrix matrix = new AdjacencyMatrix(idMap.size(), false);

        if (batchSize == -1) {
            int comparisons = 0;
            
            progressLogger.log("Creating AdjacencyMatrix");
            for (int i = numPrevFeatures; i < nodeCount; i++) {
                for (int j = 0; j < i; j++) {
                    INDArray emb1 = embedding.getColumn(i);
                    INDArray emb2 = embedding.getColumn(j);

                    double score = score(emb1, emb2);
                    comparisons++;
                    matrix.addOutgoing(idMap.get(i), idMap.get(j));
                    relWeights.put(RawValues.combineIntInt(idMap.get(i), idMap.get(j)), score);
                }
            }
            progressLogger.log("Created Adjacency Matrix");
            progressLogger.log("Number of comparisons: " + comparisons);
        } else {

            List<Future<FeatureRelationships>> futures = new ArrayList<>(concurrency);

            progressLogger.log("Creating AdjacencyMatrix");
            int offset = 0;
            boolean working = true;
            do {
                int skip = offset;
                int start = numPrevFeatures + offset;
                int end = Math.min(nodeCount, numPrevFeatures + offset + batchSize);
                progressLogger.log("Submitting job for " + start + "->" + end);
                futures.add(executor.submit(() -> loadFeatureRelationships(numPrevFeatures, skip, batchSize, idMap, embedding)));
                offset += batchSize;
                if (futures.size() >= concurrency) {
                    for (Future<FeatureRelationships> future : futures) {
                        FeatureRelationships result = get("Error building feature matrix", future);
                        working = result.rows > 0;
                        if (working) {
                            result.matrix.nodesWithRelationships(Direction.OUTGOING).forEachNode(node -> {
                                result.matrix.forEach(node, Direction.OUTGOING,
                                        (source, target, relationship) -> {
                                            matrix.addOutgoing(source, target);
                                            relWeights.put(relationship, result.relWeights.get(relationship));
                                            return true;
                                        });
                                return true;
                            });
                        }
                    }
                    futures.clear();
                }
            } while (working);
        }

        progressLogger.log("Created AdjacencyMatrix");

        return new HeavyGraph(idMap, matrix, relWeights, null);
    }

    private FeatureRelationships loadFeatureRelationships(int numPrevFeatures, int offset, int batchSize,
                                                          IdMap idMap, INDArray embedding) {

        long startTime = System.currentTimeMillis();
        int nodeCount = idMap.size();
        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount, false);
        WeightMap relWeights = new WeightMap(batchSize, 0, -1);

        int start = numPrevFeatures + offset;
        int end = Math.min(nodeCount, numPrevFeatures + offset + batchSize);
        for (int i = start; i < end; i++) {
            for (int j = 0; j < i; j++) {
                INDArray emb1 = embedding.getColumn(i);
                INDArray emb2 = embedding.getColumn(j);

                double score = score(emb1, emb2);
                matrix.addOutgoing(idMap.get(i), idMap.get(j));
                relWeights.put(RawValues.combineIntInt(idMap.get(i), idMap.get(j)), score);
            }
        }

        long endTime = System.currentTimeMillis();

        progressLogger.log(String.format("[%s] Processed %d-> %d in %d",
                Thread.currentThread().getName(), start, end, endTime - startTime));

        return new FeatureRelationships(matrix, relWeights, end - start);
    }

    private <T> T get(String message, Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted: " + message, e);
        } catch (ExecutionException e) {
            throw new RuntimeException(message, e);
        }
    }

    static class FeatureRelationships {
        private final AdjacencyMatrix matrix;
        private WeightMapping relWeights;
        private final int rows;

        FeatureRelationships(AdjacencyMatrix matrix, WeightMap relWeights, int rows) {
            this.matrix = matrix;
            this.relWeights = relWeights;
            this.rows = rows;
        }

        @Override
        public String toString() {
            return "FeatureRelationships{" +
                    "matrix=" + matrix +
                    ", relWeights=" + relWeights +
                    ", rows=" + rows +
                    '}';
        }
    }

    private INDArray pruneEmbedding(INDArray origEmbedding, int... featIdsToKeep) {
        INDArray ndPrunedEmbedding = Nd4j.create(origEmbedding.shape());
        Nd4j.copy(origEmbedding, ndPrunedEmbedding);
        return ndPrunedEmbedding.getColumns(featIdsToKeep);
    }


    public static class Feature {
        private final String name;
        private final Feature prev;

        public Feature(String name, Feature prev) {
            this.name = name;
            this.prev = prev;
        }

        public Feature(String name) {
            this.prev = null;
            this.name = name;
        }

        @Override
        public String toString() {
            return prev == null ? name : name + "( " + prev.toString() + ")";
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof Feature) && toString().equals(obj.toString());
        }
    }

    static class Embedding {
        private INDArray ndEmbedding;
        private Feature[] features;

        public Embedding(Feature[] Features, INDArray ndEmbedding) {
            this.features = Features;
            this.ndEmbedding = ndEmbedding;
        }

        public Feature[] getFeatures() {
            return features;
        }

        public INDArray getNDEmbedding() {
            return ndEmbedding;
        }
    }

    double score(INDArray feat1, INDArray feat2) {
        return feat1.eq(feat2).sum(0).getDouble(0, 0) / feat1.size(0);
    }


}

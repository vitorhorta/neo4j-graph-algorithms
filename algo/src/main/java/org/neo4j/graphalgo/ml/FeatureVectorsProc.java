package org.neo4j.graphalgo.ml;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.QueueBasedSpliterator;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.FeatureVectorExporter;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.*;

public class FeatureVectorsProc {

    @Context
    public KernelTransaction transaction;

    @Context
    public GraphDatabaseAPI api;

    @Procedure("algo.ml.featureVector.stream")
    public Stream<FeatureVectorResult> featureVectorStream(@Name(value = "availableValues") List<Object> availableValues,
                                                           @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
                                                           @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (availableValues == null || data == null) {
            return Stream.empty();
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Object[] availableValuesArray = availableValues.toArray();

        int concurrency = configuration.getConcurrency();
        return generateFeatureVectors(data, availableValuesArray, concurrency);
    }

    @Procedure(value = "algo.ml.featureVector", mode = Mode.WRITE)
    public Stream<FeatureVectorResultSummary> featureVectorWriteProc(@Name(value = "availableValues") List<Object> availableValues,
                                                                     @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
                                                                     @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (availableValues == null || data == null) {
            return Stream.empty();
        }

        Builder builder = new Builder();

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Object[] availableValuesArray = availableValues.toArray();

        int concurrency = configuration.getConcurrency();
        Stream<FeatureVectorResult> featureVectors = generateFeatureVectors(data, availableValuesArray, concurrency);

        String propertyName = configuration.getWriteProperty("featureVector");
        builder.withNodeCount(data.size()).withWrite(configuration.isWriteFlag()).withWriteProperty(propertyName);

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> new FeatureVectorExporter(api, propertyName).export(featureVectors));
        }

        return Stream.of(builder.build());
    }

    private Stream<FeatureVectorResult> generateFeatureVectors(@Name(value = "data", defaultValue = "null") List<Map<String, Object>> data, Object[] availableValuesArray, int concurrency) {
        if (concurrency == 1) {
            return sequentialFeatureVectors(data, availableValuesArray);
        } else {
            return parallelFeatureVectors(data, availableValuesArray, concurrency);
        }
    }

    private Stream<FeatureVectorResult> sequentialFeatureVectors(@Name(value = "data", defaultValue = "null") List<Map<String, Object>> data, Object[] availableValuesArray) {
        FeatureVectorResult[] featureVectorResults = new FeatureVectorResult[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {
            Map<Object, Double> weightsMap = ((List<Map<String, Object>>) row.get("selectedValues")).stream()
                    .collect(Collectors.toMap(s -> s.get("item"), this::extractWeight, (x1, x2) -> x1));

            List<Double> featureVector = LongStream.range(0, availableValuesArray.length)
                    .mapToDouble(index -> weightsMap.getOrDefault(availableValuesArray[(int) index], 0.0D)).boxed()
                    .collect(Collectors.toList());

            featureVectorResults[idx++] = new FeatureVectorResult((long) row.get("item"), featureVector);
        }
        return Stream.of(featureVectorResults);
    }

    private Stream<FeatureVectorResult> parallelFeatureVectors(@Name(value = "data", defaultValue = "null") List<Map<String, Object>> data, Object[] availableValuesArray, int concurrency) {
        int timeout = 100;
        int queueSize = 1000;
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int batchSize = ParallelUtil.adjustBatchSize(data.size(), concurrency, 100);
        Collection<Runnable> tasks = new ArrayList<>((data.size() / batchSize) + 1);

        ArrayBlockingQueue<FeatureVectorResult> queue = new ArrayBlockingQueue<>(queueSize);

        Iterator<Map<String, Object>> dataIterator = data.iterator();
        while (dataIterator.hasNext()) {

            List<Map<String, Object>> ids = new ArrayList<>(batchSize);
            int i = 0;
            while (i < batchSize && dataIterator.hasNext()) {
                ids.add(i++, dataIterator.next());
            }
            int size = i;
            tasks.add(() -> {
                for (int j = 0; j < size; j++) {
                    put(queue, createFeatureVector(availableValuesArray, ids.get(j)));
                }
            });
        }
        new Thread(() -> {
            ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            put(queue, FeatureVectorResult.TOMB);
        }).start();

        QueueBasedSpliterator<FeatureVectorResult> spliterator = new QueueBasedSpliterator<>(queue, FeatureVectorResult.TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }

    private FeatureVectorResult createFeatureVector(Object[] availableValuesArray, Map<String, Object> row) {
        Map<Object, Double> weightsMap = ((List<Map<String, Object>>) row.get("selectedValues")).stream()
                .collect(Collectors.toMap(s -> s.get("item"), this::extractWeight, (x1, x2) -> x1));

        List<Double> featureVector = LongStream.range(0, availableValuesArray.length)
                .mapToDouble(index -> weightsMap.getOrDefault(availableValuesArray[(int) index], 0.0D)).boxed()
                .collect(Collectors.toList());

        return new FeatureVectorResult((long) row.get("item"), featureVector);
    }

    private static <T> void put(BlockingQueue<T> queue, T items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static class Builder extends AbstractResultBuilder<FeatureVectorResultSummary> {


        private boolean write;
        private String writeProperty;
        private long nodes;

        public Builder withWrite(boolean write) {
            this.write = write;
            return this;
        }

        public Builder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }

        public Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public FeatureVectorResultSummary build() {
            return new FeatureVectorResultSummary(write, writeProperty, nodes, writeDuration);
        }
    }

    public static class FeatureVectorResultSummary {
        public boolean write;
        public String writeProperty;
        public long nodeCount;
        public long writeDuration;

        public FeatureVectorResultSummary(boolean write, String writeProperty, long nodeCount,
                                          long writeDuration) {
            this.write = write;
            this.writeProperty = writeProperty;
            this.nodeCount = nodeCount;
            this.writeDuration = writeDuration;
        }
    }

    public static class FeatureVectorResult {
        public static FeatureVectorResult TOMB = new FeatureVectorResult(-1, Collections.emptyList());

        public long nodeId;
        public List<Double> featureVector;

        public FeatureVectorResult(long nodeid, List<Double> featureVector) {
            this.nodeId = nodeid;
            this.featureVector = featureVector;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FeatureVectorResult that = (FeatureVectorResult) o;
            return nodeId == that.nodeId &&
                    Objects.equals(featureVector, that.featureVector);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, featureVector);
        }
    }

    private double extractWeight(Map<String, Object> map) {
        Object weight = map.get("weight");

        if (weight == null) {
            return 0.0;
        }

        if (weight instanceof Integer) {
            return ((Integer) weight).doubleValue();
        }
        if (weight instanceof Long) {
            return ((Long) weight).doubleValue();
        }

        return (Double) weight;
    }
}

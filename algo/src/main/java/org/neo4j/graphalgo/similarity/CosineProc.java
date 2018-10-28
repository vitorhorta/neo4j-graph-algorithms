/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class CosineProc extends SimilarityProc {

    @Procedure(name = "algo.similarity.cosine.stream", mode = Mode.READ)
    @Description("CALL algo.similarity.cosine.stream([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD item1, item2, count1, count2, intersection, similarity - computes cosine distance")
    // todo count1,count2 = could be the non-null values, intersection the values where both are non-null?
    public Stream<SimilarityResult> cosineStream(
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        String graphName = configuration.getGraphName("dense");

        if ("dense".equals(graphName)) {
            List<Map<String, Object>> data = (List<Map<String, Object>>) rawData;

            SimilarityComputer<DenseWeightedInput> computer = (s, t, cutoff) -> s.cosineSquares(cutoff, t);

            DenseWeightedInput[] inputs = prepareDenseWeights(data, getDegreeCutoff(configuration));

            double similarityCutoff = getSimilarityCutoff(configuration);
            // as we don't compute the sqrt until the end
            if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;

            int topN = getTopN(configuration);
            int topK = getTopK(configuration);

            Stream<SimilarityResult> stream = topN(similarityStream(inputs, computer, configuration, similarityCutoff, topK), topN);

            return stream.map(SimilarityResult::squareRooted);
        } else {
            SimilarityComputer<SparseWeightedInput> computer = (s, t, cutoff) -> s.cosineSquares(cutoff, t);

            Result result = api.execute((String) rawData);
            Map<Long, List<SparseEntry>> data = result.stream()
                    .map(row -> new SparseEntry((Long) row.get("item"), (Long) row.get("id"), extractValue(row)))
                    .collect(Collectors.groupingBy(SparseEntry::item));

            SparseWeightedInput[] inputs = prepareSparseWeights(data, getDegreeCutoff(configuration));

            double similarityCutoff = getSimilarityCutoff(configuration);
            // as we don't compute the sqrt until the end
            if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;

            int topN = getTopN(configuration);
            int topK = getTopK(configuration);

            Stream<SimilarityResult> stream = topN(similarityStream(inputs, computer, configuration, similarityCutoff, topK), topN);

            return stream.map(SimilarityResult::squareRooted);
        }
    }

    private double extractValue(Map<String, Object> row) {
        Object rawWeight = row.get("weight");

        if(rawWeight instanceof  Long) {
            return ((Long) rawWeight).doubleValue();
        }

        return (double) rawWeight;
    }

    class SparseEntry {
        private final long item;
        private final long id;
        private final double weight;

        public SparseEntry(long item, long id, double weight) {
            this.item = item;
            this.id = id;
            this.weight = weight;
        }

        public long item() {
            return item;
        }

        public long id() {
            return id;
        }

        public double weight() {
            return weight;
        }

        @Override
        public String toString() {
            return "SparseEntry{" +
                    "item=" + item +
                    ", id=" + id +
                    ", weight=" + weight +
                    '}';
        }
    }

    @Procedure(name = "algo.similarity.cosine", mode = Mode.WRITE)
    @Description("CALL algo.similarity.cosine([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD p50, p75, p90, p99, p999, p100 - computes cosine similarities")
    public Stream<SimilaritySummaryResult> cosine(
            @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        SimilarityComputer<DenseWeightedInput> computer = (s, t, cutoff) -> s.cosineSquares(cutoff, t);

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        DenseWeightedInput[] inputs = prepareDenseWeights(data, getDegreeCutoff(configuration));

        double similarityCutoff = getSimilarityCutoff(configuration);
        // as we don't compute the sqrt until the end
        if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;

        int topN = getTopN(configuration);
        int topK = getTopK(configuration);

        Stream<SimilarityResult> stream = topN(similarityStream(inputs, computer, configuration, similarityCutoff, topK), topN)
                .map(SimilarityResult::squareRooted);


        boolean write = configuration.isWriteFlag(false) && similarityCutoff > 0.0;
        return writeAndAggregateResults(configuration, stream, inputs.length, write, "SIMILAR");
    }



}

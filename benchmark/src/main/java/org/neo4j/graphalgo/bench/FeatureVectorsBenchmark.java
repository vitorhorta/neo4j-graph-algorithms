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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.ml.FeatureVectors;
import org.neo4j.helpers.collection.MapUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 1000)
@Measurement(iterations = 10000, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FeatureVectorsBenchmark {

    private List<Object> availableValues;
    private List<Map<String, Object>> weights;

    @Param({"0.1", "0.2"})
    public double percentageSelected;

    @Benchmark
    public void featureVector(Blackhole bh) throws Exception {
        bh.consume(new FeatureVectors().featureVector(availableValues, weights));
    }

    @Setup
    public void setup() throws IOException {
        int size = 10000;
        availableValues = Arrays.asList(generate(size));
        weights = generateWeights(size, 7);
    }

    private List<Map<String, Object>> generateWeights(int size, long seed) {
        Random random = new Random(seed);
        List<Map<String, Object>> weights = new ArrayList<>();
        for (int i=0;i<size * percentageSelected;i++) {
            int id = random.nextInt(size);
            int weight = random.nextInt(5);
            weights.add(MapUtil.genericMap("id", id, "weight", weight));
        }
        return weights;
    }

    private Object[] generate(int size) {
        Object[] result = new Object[size];
        for (int i=0;i<size;i++) {
            result[i] = i;
        }
        return result;
    }

    @TearDown
    public void shutdown() {
    }
}

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
package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import org.neo4j.graphalgo.api.VectorMapping;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Arrays;

/**
 * single weight cache
 */
public final class VectorMap implements VectorMapping {

    private final int capacity;
    private LongObjectHashMap<double[]> weights;
    private final double[] defaultValue;
    private final int propertyId;

    public VectorMap(
            int capacity,
            double[] defaultValue,
            int propertyId) {
        this.capacity = capacity;
        this.defaultValue = defaultValue;
        this.weights = new LongObjectHashMap<>();
        this.propertyId = propertyId;
    }

    public VectorMap(
            int capacity,
            LongObjectHashMap<double[]> weights,
            double[] defaultValue,
            int propertyId) {
        this.capacity = capacity;
        this.weights = weights;
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
    }

    /**
     * return the weight for id or defaultValue if unknown
     */
    @Override
    public double[] get(long id) {
        return weights.getOrDefault(id, defaultValue);
    }

    @Override
    public double[] get(final long id, final double[] defaultValue) {
        return weights.getOrDefault(id, defaultValue);
    }

    @Override
    public void set(long id, Object value) {
        double[] doubleVal = RawValues.extractValue(value, new double[]{});
        if (doubleVal == defaultValue) {
            return;
        }
        put(id, doubleVal);
    }

    public void put(long key, double[] value) {
        weights.put(key, value);
    }

    /**
     * return the capacity
     */
    int capacity() {
        return capacity;
    }

    /**
     * return primitive map for the weights
     */
    public LongObjectHashMap<double[]> weights() {
        return weights;
    }

    public int propertyId() {
        return propertyId;
    }

    @Override
    public int size() {
        return weights.size();
    }

    public double[] defaultValue() {
        return defaultValue;
    }
}

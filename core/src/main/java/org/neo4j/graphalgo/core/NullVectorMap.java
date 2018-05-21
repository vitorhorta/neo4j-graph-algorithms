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

import org.neo4j.graphalgo.api.VectorMapping;
import org.neo4j.graphalgo.api.WeightMapping;

/**
 * WeightMapping implementation which always returns
 * a given default weight upon invocation
 *
 * @author mknblch
 */
public class NullVectorMap implements VectorMapping {

    private final double[] defaultValue;

    public NullVectorMap(double[] defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public double[] get(long id) {
        return defaultValue;
    }

    @Override
    public double[] get(final long id, final double[] defaultValue) {
        return defaultValue;
    }

    @Override
    public double[] get(final int id) {
        return defaultValue;
    }

    @Override
    public double[] get(final int id, final double[] defaultValue) {
        return defaultValue;
    }

    @Override
    public void set(long id, Object weight) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return 0;
    }
}

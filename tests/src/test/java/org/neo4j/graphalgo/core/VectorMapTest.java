package org.neo4j.graphalgo.core;

import org.junit.Test;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.neo4j.graphalgo.core.utils.RawValues.combineIntInt;

public class VectorMapTest {
    @Test
    public void shouldA() throws Exception {
        // given
        VectorMap map = new VectorMap(100, new double[]{}, -2);

        map.set(0L, new double[] { 1,2,3});

        double[] retrieved= map.get(0);

        System.out.println("retrieved = " + Arrays.toString(retrieved));

        // when

        // then
    }

    @Test
    public void shouldB() throws Exception {
        // given
        WeightMap map = new WeightMap(100, -1, -2);

        map.set(0L, 5);

        double retrieved= map.get(0);

        System.out.println("retrieved = " + retrieved);


        // when

        // then
    }

    @Test
    public void shouldBlah() throws Exception {
        // given
        long out = combineIntInt(1, -1);
        System.out.println("out = " + out);

        // when

        // then
    }
}

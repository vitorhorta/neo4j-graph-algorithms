package org.neo4j.graphalgo.similarity;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Precision;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(JUnitQuickcheck.class)
public class ReadRlePropertyBasedTest {

    @Property
    public void mixedRepeats(List<@InRange(min = "0", max = "2") @Precision(scale=0) Number> vector1List, @InRange(min="1", max="3") int limit) throws Exception {
        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, limit);
        System.out.println(vector1List);
        System.out.println(Arrays.toString(vector1Rle));

        // then
        ReadRle readRle = new ReadRle(vector1Rle);

        for (Number value : vector1List) {
            readRle.next();
            assertEquals(value.doubleValue(), readRle.value(), 0.001);
        }
    }
}

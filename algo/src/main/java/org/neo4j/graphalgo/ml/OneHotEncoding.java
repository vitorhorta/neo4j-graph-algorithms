package org.neo4j.graphalgo.ml;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class OneHotEncoding {

    @UserFunction("algo.ml.oneHotEncoding")
    @Description("CALL algo.ml.oneHotEncoding(categories, selectedCategories) - return a list of selected values in a one hot encoding format.")
    public List<Long> oneHotEncoding(@Name(value = "categories") List<Object> values,
                                     @Name(value = "selectedCategories") List<Object> selectedValues) {

        return LongStream.range(0, values.size())
                .map(index -> selectedValues.contains(values.get((int) index)) ? 1L : 0L)
                .boxed()
                .collect(Collectors.toList());
    }
}

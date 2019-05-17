package com.river.mydemo.batch;

import com.google.gson.Gson;
import lombok.val;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author river
 * @date 2019/5/17 15:28
 **/
public class CoGroupTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, Integer>> iVals = env.fromElements(
                new Tuple2<String, Integer>("river", 30),
                new Tuple2<String, Integer>("lucy", 20),
                new Tuple2<String, Integer>("frank", 33),
                new Tuple2<String, Integer>("cat", 5));

        DataSet<Tuple2<String, Double>> dVals = env.fromElements(
                new Tuple2<String, Double>("river", 1d),
                new Tuple2<String, Double>("lucy", 1d),
                new Tuple2<String, Double>("lucy", 1d),
                new Tuple2<String, Double>("frank", 1d),
                new Tuple2<String, Double>("cat", 1d));

        DataSet<Double> output = (DataSet<Double>) iVals.coGroup(dVals)
                // group first DataSet on first tuple field
                .where(0)
                // group second DataSet on first tuple field
                .equalTo(0)
                // apply CoGroup function on each pair of groups
                .with(new MyCoGrouper());

        output.print();

    }

    public static class MyCoGrouper
            implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Double>, Double> {

        @Override
        public void coGroup(Iterable<Tuple2<String, Integer>> iVals,
                            Iterable<Tuple2<String, Double>> dVals,
                            Collector<Double> out) {

            Set<Integer> ints = new HashSet<Integer>();
            System.out.println("---------------------");
            System.out.println("ivals = " + new Gson().toJson(iVals));
            System.out.println("dVals = " + new Gson().toJson(dVals));
            // add all Integer values in group to set
            for (Tuple2<String, Integer> val : iVals) {
                ints.add(val.f1);
            }

            // multiply each Double value with each unique Integer values of group
            for (Tuple2<String, Double> val : dVals) {
                for (Integer i : ints) {
                    out.collect(val.f1 * i);
                }
            }
        }
    }
}

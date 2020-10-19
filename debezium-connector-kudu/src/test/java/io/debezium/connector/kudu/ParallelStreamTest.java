package io.debezium.connector.kudu;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

public class ParallelStreamTest {

    @Test
    public void testCollectWithOriginalOrdered() {
        List<Integer> original = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            original.add(i);
        }

        long begin = System.currentTimeMillis();
        List<Double> collect = original.stream().map((it) -> {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            return it * 1.0;
        }).collect(Collectors.toList());
        System.out.println(collect.size());
        System.out.println("costs:" + (System.currentTimeMillis() - begin));

        long parallelBegin = System.currentTimeMillis();
        List<Double> parallelCollect = original.parallelStream().map((it) -> {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            return it * 1.0;
        }).collect(Collectors.toList());
        System.out.println(parallelCollect.size());
        System.out.println("parallel costs:" + (System.currentTimeMillis() - parallelBegin));

        for (double item : parallelCollect) {
            System.out.println(item);
        }
    }

}

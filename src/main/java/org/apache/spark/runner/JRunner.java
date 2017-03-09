package org.apache.spark.runner;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SchedulerBackend;
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend;
import org.apache.spark.scheduler.local.LocalSchedulerBackend;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.rangeClosed;

public class JRunner implements Serializable {

    private static int numOfSparkExecutors(JavaSparkContext sparkContext) {
        if (sparkContext.isLocal())
            return 1;
        else {
            SchedulerBackend sb = sparkContext.sc().schedulerBackend();
            if (sb instanceof LocalSchedulerBackend)
                return 1;
            else if (sb instanceof CoarseGrainedSchedulerBackend)
                return ((CoarseGrainedSchedulerBackend) sb).getExecutorIds().length();
            else
                return sparkContext.sc().getExecutorStorageStatus().length - 1;
        }
    }

    public static List<String> getNodes(JavaSparkContext sparkContext) {
        int numNodes = numOfSparkExecutors(sparkContext);
        List<Integer> il = rangeClosed(1, numNodes).boxed().collect(Collectors.toList());
        JavaRDD<Integer> rdd = sparkContext.parallelize(il, numNodes);
        return rdd.mapPartitions(it -> new Iterator<String>() {
            Boolean firstTime = true;

            @Override
            public boolean hasNext() {
                if (firstTime) {
                    firstTime = false;
                    return true;
                } else
                    return firstTime;
            }

            @Override
            public String next() {
                InetAddress address;
                try {
                    address = InetAddress.getLocalHost();
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                return address.getHostAddress();
            }
        }, true).distinct().collect();
    }

    public static List<String> executeOnNodes(JavaSparkContext sparkContext, Supplier<String> func) {
        int numNodes = numOfSparkExecutors(sparkContext);
        List<Integer> il = rangeClosed(1, numNodes).boxed().collect(Collectors.toList());
        JavaRDD<Integer> rdd = sparkContext.parallelize(il, numNodes);
        return rdd.mapPartitions(it -> new Iterator<String>() {
            Boolean firstTime = true;

            @Override
            public boolean hasNext() {
                if (firstTime) {
                    firstTime = false;
                    return true;
                } else
                    return firstTime;
            }

            @Override
            public String next() {
                return func.get();
            }
        }, true).distinct().collect();
    }

}

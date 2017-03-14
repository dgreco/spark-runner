package org.apache.spark.runner;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.rangeClosed;

public class JRunner<T> implements Serializable {

    public static List<String> getNodes(JavaSparkContext sparkContext) {
        int numNodes = package$.MODULE$.numOfSparkExecutors(sparkContext.sc());
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

    public static <T> T[] executeOnNodes(java.util.function.Function<ExecutionContext, T> func, JavaSparkContext sparkContext) {
        return (T[]) package$.MODULE$.jexecuteOnNodes(func, sparkContext);
    }

    public static JavaDStream<Tuple2<String, String>> jstreamingExecuteOnNodes(java.util.function.Consumer<StreamingExecutionContext> func, JavaStreamingContext streamingContext) {
        return package$.MODULE$.jstreamingExecuteOnNodes(func, streamingContext);
    }

}

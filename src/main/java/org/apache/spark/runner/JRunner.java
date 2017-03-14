package org.apache.spark.runner;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class JRunner {

    public static String[] getNodes(JavaSparkContext sparkContex) {
        return package$.MODULE$.getNodes(sparkContex.sc());
    }

    public static <T> T[] executeOnNodes(java.util.function.Function<ExecutionContext, T> func, JavaSparkContext sparkContext) {
        return (T[]) package$.MODULE$.jexecuteOnNodes(func, sparkContext);
    }

    public static JavaDStream<Tuple2<String, String>> streamingExecuteOnNodes(java.util.function.Consumer<StreamingExecutionContext> func, JavaStreamingContext streamingContext) {
        return package$.MODULE$.jstreamingExecuteOnNodes(func, streamingContext);
    }

}

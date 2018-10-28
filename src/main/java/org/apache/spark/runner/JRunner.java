/*
 * Copyright 2018 David Greco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.runner;

import org.apache.spark.api.java.JavaSparkContext;

public class JRunner {

    public static String[] getNodes(JavaSparkContext sparkContex) {
        return package$.MODULE$.getNodes(sparkContex.sc());
    }

    public static <T> T[] executeOnNodes(java.util.function.Function<ExecutionContext, T> func, JavaSparkContext sparkContext) {
        return (T[]) package$.MODULE$.jexecuteOnNodes(func, sparkContext);
    }

}

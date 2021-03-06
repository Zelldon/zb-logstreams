/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zeebe.logstreams.reader.benchmarks;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

public class Benchmarks
{
    public static final int DATA_SET_SIZE = 100_000;

    public static void main(String... args) throws Exception
    {

        final Options opts = new OptionsBuilder()
                .include(".*LogStream")
                .warmupIterations(5)
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(30))
                .jvmArgs("-server")
                .forks(1)
                .build();

        new Runner(opts).run();

    }
}

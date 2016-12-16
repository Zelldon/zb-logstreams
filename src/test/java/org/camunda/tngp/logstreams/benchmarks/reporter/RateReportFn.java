package org.camunda.tngp.logstreams.benchmarks.reporter;

@FunctionalInterface
public interface RateReportFn
{

    void reportRate(long timestamp, long intervalValue);

}

package org.camunda.tngp.logstreams.benchmarks.reporter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class RateReporter
{
    protected volatile long value = 0;

    protected long lastReportedValue = 0;
    protected long startTime = 0;

    protected final long reportIntervalInNs;

    protected final RateReportFn reportFn;

    protected volatile boolean exit = false;

    public RateReporter(int interval, TimeUnit intervalUnit, RateReportFn reportFn)
    {
        this.reportFn = reportFn;
        this.reportIntervalInNs = intervalUnit.toNanos(interval);
        this.startTime = System.nanoTime();
    }

    public void doReport()
    {
        do
        {
            final long now = System.nanoTime();
            final long currentValue = value;
            final long intervalValue = currentValue - lastReportedValue;
            final long timestamp = now - startTime;

            reportFn.reportRate(timestamp, intervalValue);

            lastReportedValue = currentValue;

            LockSupport.parkNanos(reportIntervalInNs);
        }
        while (!exit);
    }

    public void increment()
    {
        ++value;
    }

    public void exit()
    {
        exit = true;
    }

}

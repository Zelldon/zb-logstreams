package org.camunda.tngp.logstreams.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.logstreams.LogStreams;
import org.camunda.tngp.logstreams.benchmarks.reporter.RateReporter;
import org.camunda.tngp.logstreams.benchmarks.reporter.SysoutRateReportFn;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.logstreams.log.LogStreamWriter;
import org.camunda.tngp.util.agent.DedicatedAgentRunnerService;
import org.camunda.tngp.util.agent.SimpleAgentRunnerFactory;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.junit.Test;

public class WriteThroughputTest
{

    protected static final int EXECUTIONS = 10;

    public static void main(String[] args) throws Exception
    {
        List<Long>[] reports = new List[EXECUTIONS];
        for (int i = 0; i < EXECUTIONS; i++) {
            reports[i] = testThrougput();
        }

        List<Long> avgReport = new ArrayList<Long>();
        for (int i = 0; i < reports[0].size(); i++) {
            long avg = 0;
            for (int j = 0; j < EXECUTIONS; j++) {
                List<Long> report = reports[j];
                if (i < report.size()) {
                    avg += report.get(i);
                }
            }
            avg /= EXECUTIONS;
            avgReport.add(avg);
        }



        Plot plot = new Plot("Throughput", avgReport);
        plot.pack( );
        RefineryUtilities.centerFrameOnScreen( plot );
        plot.setVisible( true );
    }

    private static class Plot extends ApplicationFrame {
        private final List<Long> avgReport;

        public Plot(String title, List<Long> avgReport) {
            super(title);
            this.avgReport = avgReport;

            final JFreeChart chart = ChartFactory.createTimeSeriesChart(
                title,      // chart title
                "Interval",                      // x axis label
                "Values",                      // y axis label
                createDataset(),
                true,                     // include legend
                true,                     // tooltips
                false                     // urls
            );

            ChartPanel chartPanel = new ChartPanel(chart);
            setContentPane(chartPanel);
        }


        private XYDataset createDataset() {
            final XYSeriesCollection dataset = new XYSeriesCollection();

            final XYSeries series1 = new XYSeries("AVG");
            int count = 0;
            for (Long avg : avgReport) {
                series1.add(count++, avg);
            }

            dataset.addSeries(series1);
            return dataset;
        }

    }




    private static List<Long> testThrougput() {
        List<Long> reportCounts = new ArrayList<Long>();
        final byte[] evt = new byte[512];
        final DirectBuffer evntBuffer = new UnsafeBuffer(evt);

        final DedicatedAgentRunnerService agentRunnerService = new DedicatedAgentRunnerService(new SimpleAgentRunnerFactory());
        final LogStream stream = LogStreams.createFsLogStream("test-stream", 0)
            .logRootPath("/home/zell/tmp/benchmark-logs")
            .deleteOnClose(true)
            .agentRunnerService(agentRunnerService)
            .writeBufferSize(16 * 1024 * 1024)
            .maxAppendBlockSize(4 * 1024 * 1024)
            .build();

        final LogStreamWriter writer = new LogStreamWriter(stream);

        stream.open();

        final RateReporter rateReporter = new RateReporter(10, TimeUnit.MILLISECONDS,  (timestamp, intervalValue) -> {
            System.out.println(intervalValue);
            reportCounts.add(intervalValue);
        });

        new Thread()
        {
            @Override
            public void run()
            {
                rateReporter.doReport();
            }

        }.start();

        try (LogStream openStream = stream)
        {

            long lastEventPosition = -1;

            final long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            long zeroCount = 0;
            long reportCount = 0;
            while (System.nanoTime() < endTime)
            {
                lastEventPosition = writer.positionAsKey()
                        .value(evntBuffer)
                        .tryWrite();

                if (lastEventPosition > 0)
                {
                    rateReporter.increment();
                    reportCount++;
                } else {
                    zeroCount++;
                }
            }

//            System.out.printf("Zerocount: %d report count: %d, ratio: %d\n", zeroCount, reportCount, zeroCount/reportCount);


            while (stream.getCurrentAppenderPosition() < lastEventPosition)
            {
                // spin
            }

        }

        rateReporter.exit();

        stream.getContext()
            .getWriteBuffer()
            .close();
        return reportCounts;
    }


}

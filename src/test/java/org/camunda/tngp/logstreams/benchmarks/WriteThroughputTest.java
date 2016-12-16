package org.camunda.tngp.logstreams.benchmarks;

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

public class WriteThroughputTest
{

    public static void main(String[] args) throws Exception
    {
        final byte[] evt = new byte[512];
        final DirectBuffer evntBuffer = new UnsafeBuffer(evt);

        final DedicatedAgentRunnerService agentRunnerService = new DedicatedAgentRunnerService(new SimpleAgentRunnerFactory());
        final LogStream stream = LogStreams.createFsLogStream("test-stream", 0)
            .logRootPath("/home/meyerd/tmp/benchmark-logs")
//            .deleteOnClose(true)
            .agentRunnerService(agentRunnerService)
            .writeBufferSize(1024 * 1024 * 1024)
            .maxAppendBlockSize(8 * 1024 * 1024)
            .build();

        final LogStreamWriter writer = new LogStreamWriter(stream);

        stream.open();

        final RateReporter rateReporter = new RateReporter(100, TimeUnit.MILLISECONDS,  new SysoutRateReportFn());

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

            while (System.nanoTime() < endTime)
            {
                lastEventPosition = writer.positionAsKey()
                        .value(evntBuffer)
                        .tryWrite();

                if (lastEventPosition > 0)
                {
                    rateReporter.increment();
                }
            }

            while (stream.getCurrentAppenderPosition() < lastEventPosition)
            {
                // spin
            }

        }

        rateReporter.exit();

        stream.getContext()
            .getWriteBuffer()
            .close();
    }


}

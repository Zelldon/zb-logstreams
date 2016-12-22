/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.tngp.logstreams.snapshot;

import java.time.Duration;

import org.camunda.tngp.logstreams.spi.SnapshotPolicy;
import org.camunda.tngp.util.time.ClockUtil;

/**
 * Creates snapshots periodically in a given time interval. The first snapshot
 * is created after one interval.
 */
public class TimeBasedSnapshotPolicy implements SnapshotPolicy
{
    protected final long snapshotIntervalInMillis;

    protected long timestampOfLastSnapshot;

    public TimeBasedSnapshotPolicy(Duration snapshotInterval)
    {
        this.snapshotIntervalInMillis = snapshotInterval.toMillis();

        // initial delay
        this.timestampOfLastSnapshot = ClockUtil.getCurrentTimeInMillis();
    }

    @Override
    public boolean apply(long logPosition)
    {
        final long currentTimeInMillis = ClockUtil.getCurrentTimeInMillis();
        final long timeSinceLastSnapshot = currentTimeInMillis - timestampOfLastSnapshot;

        final boolean isTimeGreaterThanInterval = timeSinceLastSnapshot >= snapshotIntervalInMillis;

        if (isTimeGreaterThanInterval)
        {
            timestampOfLastSnapshot = currentTimeInMillis;
        }

        return isTimeGreaterThanInterval;
    }

}
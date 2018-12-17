package app_kvEcs;

/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PhiAccrualFailureDetector {
    /*
    https://github.com/komamitsu/phi-accural-failure-detector/blob/master/src/main/java/org/komamitsu/failuredetector/PhiAccuralFailureDetector.java
    */
    private final double threshold;
    private final double minStdDeviationMillis;
    private final long acceptableHeartbeatPauseMillis;
    private final HeartbeatHistory heartbeatHistory;
    private final AtomicReference<Long> lastTimestampMillis = new AtomicReference<Long>();

    public PhiAccrualFailureDetector(double threshold, int samplingSize, double minStdDeviationMillis, long acceptableHeartbeatPauseMillis, long firstHeartbeatEstimateMillis) {
        if (threshold <= 0) throw new IllegalArgumentException("Threshold must be > 0: " + threshold);

        if (samplingSize <= 0) throw new IllegalArgumentException("Sample size must be > 0: " + samplingSize);

        if (minStdDeviationMillis <= 0)
            throw new IllegalArgumentException("Minimum standard deviation must be > 0: " + minStdDeviationMillis);

        if (acceptableHeartbeatPauseMillis < 0)
            throw new IllegalArgumentException("Acceptable heartbeat pause millis must be >= 0: " + acceptableHeartbeatPauseMillis);

        if (firstHeartbeatEstimateMillis <= 0)
            throw new IllegalArgumentException("First heartbeat value must be > 0: " + firstHeartbeatEstimateMillis);

        this.threshold = threshold;
        this.minStdDeviationMillis = minStdDeviationMillis;
        this.acceptableHeartbeatPauseMillis = acceptableHeartbeatPauseMillis;

        long stdDeviationMillis = firstHeartbeatEstimateMillis / 4;
        heartbeatHistory = new HeartbeatHistory(samplingSize);
        heartbeatHistory.add(firstHeartbeatEstimateMillis - stdDeviationMillis).add(firstHeartbeatEstimateMillis + stdDeviationMillis);
    }

    private double ensureValidStdDeviation(double stdDeviationMillis) {
        return Math.max(stdDeviationMillis, minStdDeviationMillis);
    }

    public synchronized double phi(long timestampMillis) {
        Long lastTimestampMillis = this.lastTimestampMillis.get();
        if (lastTimestampMillis == null) {
            return 0.0;
        }

        long timeDiffMillis = timestampMillis - lastTimestampMillis;
        double meanMillis = heartbeatHistory.mean() + acceptableHeartbeatPauseMillis;
        double stdDeviationMillis = ensureValidStdDeviation(heartbeatHistory.stdDeviation());
        double y = (timeDiffMillis - meanMillis) / stdDeviationMillis;
        double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
        if (timeDiffMillis > meanMillis) {
            return -Math.log(e / (1.0 + e));
        } else {
            return -Math.log(1.0 - 1.0 / (1.0 + e));
        }
    }

    public synchronized double phi() {
        return phi(System.currentTimeMillis());
    }

    public boolean isAvailable() {
        return phi(System.currentTimeMillis()) < threshold;
    }

    public boolean isAvailable(long timestampMillis) {
        return phi(timestampMillis) < threshold;
    }

    public synchronized void heartbeat(long timestampMillis) {
        Long lastTimestampMillis = this.lastTimestampMillis.getAndSet(timestampMillis);
        if (lastTimestampMillis != null) {
            long interval = timestampMillis - lastTimestampMillis;
            if (isAvailable(timestampMillis)) heartbeatHistory.add(interval);
        }
    }

    public void heartbeat() {
        heartbeat(System.currentTimeMillis());
    }

    private static class HeartbeatHistory {
        private final int maxSampleSize;
        private final LinkedList<Long> intervals = new LinkedList<Long>();
        private final AtomicLong intervalSum = new AtomicLong();
        private final AtomicLong squaredIntervalSum = new AtomicLong();

        public HeartbeatHistory(int maxSampleSize) {
            if (maxSampleSize < 1) {
                throw new IllegalArgumentException("maxSampleSize must be >= 1, got " + maxSampleSize);
            }
            this.maxSampleSize = maxSampleSize;
        }

        public double mean() {
            return (double) intervalSum.get() / intervals.size();
        }

        public double variance() {
            return ((double) squaredIntervalSum.get() / intervals.size()) - (mean() * mean());
        }

        public double stdDeviation() {
            return Math.sqrt(variance());
        }

        public HeartbeatHistory add(long interval) {
            if (intervals.size() >= maxSampleSize) {
                Long dropped = intervals.pollFirst();
                intervalSum.addAndGet(-dropped);
                squaredIntervalSum.addAndGet(-pow2(dropped));
            }
            intervals.add(interval);
            intervalSum.addAndGet(interval);
            squaredIntervalSum.addAndGet(pow2(interval));
            return this;
        }

        private long pow2(Long x) {
            return x * x;
        }
    }
}



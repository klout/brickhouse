package brickhouse.udf.hll;

/**
 * Copyright 2012,2013 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import io.airlift.slice.Slices;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.log4j.Logger;
import io.airlift.stats.cardinality.HyperLogLog;

public class HLLBuffer implements AggregationBuffer {
    private static final Logger LOG = Logger.getLogger(HLLBuffer.class);
    private HyperLogLog hll;
    private int precision;

    public HLLBuffer() {
        hll = null;
        precision = 0;
    }

    public boolean isReady() {
        return precision != 0;
    }

    public void init(int precision) {
        this.precision = precision;
        hll = HyperLogLog.newInstance(1<<precision);
    }

    public void reset() {
        hll = null;
        precision = 0;
    }

    public void addItem(String str) {
        hll.add(Slices.utf8Slice(str));
    }

    public void merge(byte[] buffer) {
        if (buffer == null) {
            return;
        }

        HyperLogLog other = HyperLogLog.newInstance(Slices.wrappedBuffer(buffer));

        if (hll == null) {
            LOG.debug("hll is null; other.sizeof = " + other.estimatedInMemorySize());
            hll = other;
            precision = (int) Math.ceil(Math.log(other.estimatedSerializedSize()) / Math.log(2.0));
            LOG.debug("precision set to: " + precision);
        } else {
            hll.mergeWith(other);
        }
    }

    public byte[] getPartial() {
        if (hll == null) {
            return null;
        }
        return hll.serialize().getBytes();
    }

}
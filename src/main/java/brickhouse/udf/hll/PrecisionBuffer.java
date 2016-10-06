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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * PrecisionBuffer to be used as the AggregationBuffer class with the
 * ValidatePrecisionUDAF aggregator.
 **/

public class PrecisionBuffer implements AggregationBuffer {
    private static final Logger LOG = Logger.getLogger(PrecisionBuffer.class);
    private int precision;
    private boolean precisionsEqual;

    public PrecisionBuffer() {
        precisionsEqual = true;
        precision = 0;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }
    public boolean isReady() {
        return precision != 0;
    }

    public void init(int precision) {
        this.precision = precision;
    }

    public void reset() {
        precisionsEqual = true;
        precision = 0;
    }

    public void merge(boolean bool) throws IOException {
        precisionsEqual = (precisionsEqual && bool);
        if (!precisionsEqual) {
            LOG.debug("HyperLogLog has different precision value.");
        }
    }

    public boolean getPartial() throws IOException {
        return precisionsEqual;
    }
}


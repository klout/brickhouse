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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

/**
 * Aggregate and return a HyperLogLog.
 * <p/>
 * Uses Clearspring's Stream-lib project
 */

@Description(name = "hyperloglog",
        value = "_FUNC_(x, [b]) - Constructs a HyperLogLog++ estimator to estimate reach for large values, " +
                "with optional bit parameter for specifying precision (b must be in [4,16])." +
                "\nDefault is b = 6." +
                "\nReturns a binary value that represents the HyperLogLog++ data structure."
)
public class HyperLogLogUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(HyperLogLogUDAF.class);
    static final int DEFAULT_PRECISION = 6;
    static final int MIN_PRECISION = 4;
    static final int MAX_PRECISION = 16;

    @SuppressWarnings("deprecation")
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        TypeInfo[] parameters = info.getParameters();

        if (parameters.length != 1 && parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Please specify one or two arguments.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName()
                            + " was passed as parameter 1.");
        }

        if (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0,
                    "Only a string argument is accepted as parameter 1, but "
                            + parameters[0].getTypeName()
                            + " was passed instead.");
        }

        if (parameters.length == 2) {
            // validate the second parameter, which is the precision value
            if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(1,
                        "Only primitive type arguments are accepted but "
                                + parameters[1].getTypeName()
                                + " was passed as parameter 2.");
            }

            if (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
                throw new UDFArgumentTypeException(1,
                        "Only an integer argument is accepted as parameter 2, but "
                                + parameters[1].getTypeName()
                                + " was passed instead.");
            }
        }

        if (parameters.length > 2) throw new IllegalArgumentException("Function only takes 1 or 2 parameters.");

        return new HyperLogLogUDAFEvaluator();
    }

    public static class HyperLogLogUDAFEvaluator extends GenericUDAFEvaluator {
        private static final Logger LOG = Logger.getLogger(HyperLogLogUDAFEvaluator.class);
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private StringObjectInspector inputStrOI;
        private IntObjectInspector inputPrecisionIntOI;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (binary serialized hll object)
        private BinaryObjectInspector partialBufferOI;

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            LOG.debug("evaluator init: mode = " + m.name());

            // init input object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // iterate() gets called.. string and int passed in
                this.inputStrOI = (StringObjectInspector) parameters[0];
                if (parameters.length == 2) {
                    this.inputPrecisionIntOI = (IntObjectInspector) parameters[1];
                }
            } else {
                // Mode m == Mode.PARTIAL2 || m == Mode.FINAL
                // merge() gets called ... serialized hll is passed in
                this.partialBufferOI = (BinaryObjectInspector) parameters[0];
            }

            // init output object inspectors
            // The partial aggregate type is the same as the final type
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {

            assert (parameters.length == 1 || parameters.length == 2);

            if ((parameters.length == 1 && parameters[0] == null)
                    || (parameters.length == 2 && (parameters[0] == null || parameters[1] == null))) {
                return;
            }

            HLLBuffer myagg = (HLLBuffer) agg;

            // initialize aggregation buffer once
            if (!myagg.isReady()) {
                LOG.debug("agg buffer is not ready");
                int p = DEFAULT_PRECISION;

                // If specified, parse out the precision and validate it is in allowed range.
                if (parameters.length == 2) {
                    p = PrimitiveObjectInspectorUtils.getInt(parameters[1], inputPrecisionIntOI);
                    if (p < MIN_PRECISION || p > MAX_PRECISION) {
                        throw new HiveException(getClass().getSimpleName() + " precision must be in [4,16],"
                                + " but you supplied " + p + ".");
                    }
                }

                LOG.debug("initializing agg buffer: p = " + p);
                // allocate memory for the histogram bins
                myagg.init(p);
            }

            // string object to be added to hll
            Object strObj = parameters[0];

            String str = inputStrOI.getPrimitiveJavaObject(strObj);
            myagg.addItem(str);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial == null) {
                return;
            }

            try {
                HLLBuffer myagg = (HLLBuffer) agg;
                HLLBuffer partialObj = (HLLBuffer) partial;
                byte[] partialBuffer = this.partialBufferOI
                        .getPrimitiveJavaObject(partialObj.getPartial());
                myagg.merge(partialBuffer);
            } catch (Exception e) {
                throw new HiveException(e);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            try {
                HLLBuffer myagg = (HLLBuffer) agg;
                return myagg.getPartial();
            } catch (Exception e) {
                throw new HiveException(e);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LOG.debug("terminatePartial");
            return terminate(agg);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LOG.debug("getNewAggregationBuffer");
            HLLBuffer buff = new HLLBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            LOG.debug("reset");
            HLLBuffer hllBuff = (HLLBuffer) buff;
            hllBuff.reset();
        }

    }
}

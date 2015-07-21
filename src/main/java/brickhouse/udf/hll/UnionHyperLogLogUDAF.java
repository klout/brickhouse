package brickhouse.udf.hll;
/**
 * Copyright 2012 Klout, Inc
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

/**
 * Aggregate multiple HyperLogLog structures together.
 */

@Description(name = "union_hyperloglog",
        value = "_FUNC_(x) - Merges multiple hyperloglogs together. "
)
public class UnionHyperLogLogUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(UnionHyperLogLogUDAF.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Please specify one argument.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName()
                            + " was passed as parameter 1.");
        }

        if (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
            throw new UDFArgumentTypeException(0,
                    "Only a binary argument is accepted as parameter 1, but "
                            + parameters[0].getTypeName()
                            + " was passed instead.");
        }

        if (parameters.length > 1) throw new IllegalArgumentException("Function only takes 1 parameter.");

        return new MergeHyperLogLogUDAFEvaluator();
    }

    public static class MergeHyperLogLogUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (binary serialized hll object)
        private BinaryObjectInspector inputAndPartialBinaryOI;

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            LOG.debug(" MergeHyperLogLogUDAF.init() - Mode= " + m.name());

            // init input object inspectors
            this.inputAndPartialBinaryOI = (BinaryObjectInspector) parameters[0];

            // init output object inspectors
            // The partial aggregate type is the same as the final type
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            HLLBuffer buff = new HLLBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            try {

                if (parameters[0] == null) {
                    return;
                }

                Object partial = parameters[0];
                merge(agg, partial);
            } catch (Exception e) {
                LOG.error("Error", e);
                throw new HiveException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial == null) {
                return;
            }

            try {
                HLLBuffer myagg = (HLLBuffer) agg;
                byte[] partialBuffer = this.inputAndPartialBinaryOI.getPrimitiveJavaObject(partial);
                myagg.merge(partialBuffer);
            } catch (Exception e) {
                LOG.error("Error", e);
                throw new HiveException(e);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            HLLBuffer hllBuff = (HLLBuffer) buff;
            hllBuff.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            try {
                HLLBuffer myagg = (HLLBuffer) agg;
                return myagg.getPartial();
            } catch (Exception e) {
                LOG.error("Error", e);
                throw new HiveException(e);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }
    }
}

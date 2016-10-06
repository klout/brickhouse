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

import com.clearspring.analytics.util.Varint;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

/**
 * Validates the precisions for a collection of HyperLogLog binaries,
 * (aggregation necessarily requires the same precision).
 * Returns true if and only if all elements have the same precision
 */

@Description(name = "validate_precisions",
        value = "_FUNC_(x) - Ensures equal precision values of HyperLogLog++ binaries. "
)
public class ValidatePrecisionUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(ValidatePrecisionUDAF.class);

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

        return new PrecisionHyperLogLogUDAFEvaluator();
    }

    public static class PrecisionHyperLogLogUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data (binary serialized hll object)
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (boolean)
        private BinaryObjectInspector inputBinaryOI;
        private BooleanObjectInspector partialBooleanOI;

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            LOG.debug(" PrecisionHyperLogLogUDAF.init() - Mode= " + m.name());

            //Input (binary) object inspector
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                this.inputBinaryOI = (BinaryObjectInspector) parameters[0];
            }
            //Partial (boolean) object inspector
            else {
                // init input object inspectors
                this.partialBooleanOI = (BooleanObjectInspector) parameters[0];
            }
            //Output object inspector
            return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            PrecisionBuffer buff = new PrecisionBuffer();
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

                Object input = parameters[0];
                int precision;
                byte[] bytes = inputBinaryOI.getPrimitiveJavaObject(input);
                DataInputStream oi = new DataInputStream(new ByteArrayInputStream(bytes));
                int version = oi.readInt();
                if (version < 0) {
                    precision = Varint.readUnsignedVarInt(oi);
                }
                else {
                    precision = oi.readInt();
                }
                PrecisionBuffer myagg = (PrecisionBuffer) agg;
                if (!myagg.isReady()) {
                    myagg.setPrecision(precision);
                }
                else {
                    myagg.merge((precision == myagg.getPrecision()));
                }
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
                PrecisionBuffer myagg = (PrecisionBuffer) agg;
                boolean bool =  (Boolean) this.partialBooleanOI.getPrimitiveJavaObject(partial);
                myagg.merge(bool);
            } catch (Exception e) {
                LOG.error("Error", e);
                throw new HiveException(e);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            PrecisionBuffer preBuff = (PrecisionBuffer) buff;
            preBuff.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            try {
                PrecisionBuffer myagg = (PrecisionBuffer) agg;
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

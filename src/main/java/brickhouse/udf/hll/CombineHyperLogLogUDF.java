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
import io.airlift.stats.cardinality.HyperLogLog;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

/**
 * Combine two HyperLogLog++ structures together.
 */
@Description(name = "combine_hyperloglog",
        value = "_FUNC_(x) - Combined two  HyperLogLog++ binary blobs. "
)
public class CombineHyperLogLogUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(CombineHyperLogLogUDF.class);

    private BinaryObjectInspector binary1Inspector;
    private BinaryObjectInspector binary2Inspector;


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        try {
            Object blobObj1 = arg0[0].get();
            Object blobObj2 = arg0[1].get();

            byte[] bref1 = this.binary1Inspector.getPrimitiveJavaObject(blobObj1);
            byte[] bref2 = this.binary2Inspector.getPrimitiveJavaObject(blobObj2);

            if (bref1 != null && bref2 != null) {
                HyperLogLog hll1 = HyperLogLog.newInstance(Slices.wrappedBuffer(bref1));
                HyperLogLog hll2 = HyperLogLog.newInstance(Slices.wrappedBuffer(bref2));

                hll1.mergeWith(hll2);
                return hll1.serialize().getBytes();
            } else {
                return null;
            }

        } catch (Exception e) {
            LOG.error("Error", e);
            throw new HiveException(e);
        }

    }

    @Override
    public String getDisplayString(String[] arg0) {
        StringBuilder sb = new StringBuilder("combine_hyperloglog( ");
        for (int i = 0; i < arg0.length - 1; ++i) {
            sb.append(arg0[i]);
            sb.append(" , ");
        }
        sb.append(arg0[arg0.length - 1]);
        sb.append(" )");
        return sb.toString();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 2) {
            throw new UDFArgumentException("combine_hyperloglog takes a pair of binary objects which were created with the hyperloglog UDAF");
        }
        if (arg0[0].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("combine_hyperloglog takes a pair of binary objects which were created with the hyperloglog UDAF");
        }
        PrimitiveObjectInspector primInsp = (PrimitiveObjectInspector) arg0[0];
        if (primInsp.getPrimitiveCategory() != PrimitiveCategory.BINARY) {
            throw new UDFArgumentException("hll_est_cardinality takes a binary object which was created with the hyperloglog UDAF");
        }
        this.binary1Inspector = (BinaryObjectInspector) arg0[0];
        this.binary2Inspector = (BinaryObjectInspector) arg0[1];

        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }


}

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

import com.clearspring.analytics.util.Varint;
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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

/**
 * Return the precision (p) value for a
 * parameter HyperLogLog++
 */
@Description(name = "hll_get_precision",
        value = "_FUNC_(x) - Determine precision of a HyperLogLog++ binary. "
)
public class GetPrecisionUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(GetPrecisionUDF.class);

    private BinaryObjectInspector binaryInspector;

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        try {
            Object blobObj = arg0[0].get();

            byte[] bref = this.binaryInspector.getPrimitiveJavaObject(blobObj);
            if (bref == null)
                return null;
            DataInputStream oi = new DataInputStream(new ByteArrayInputStream(bref));
            int version = oi.readInt();
            if (version < 0) {
                return Varint.readUnsignedVarInt(oi);
            }
            else {
                return oi.readInt();
            }
        } catch (Exception e) {
            LOG.error("Unable to determine precision", e);
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        StringBuilder sb = new StringBuilder("hll_get_precision( ");
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
        if (arg0.length != 1) {
            throw new UDFArgumentException("hll_get_precision takes a binary object which was created with the hyperloglog UDAF");
        }
        if (arg0[0].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("hll_get_precision takes a binary object which was created with the hyperloglog UDAF");
        }
        PrimitiveObjectInspector primInsp = (PrimitiveObjectInspector) arg0[0];
        if (primInsp.getPrimitiveCategory() != PrimitiveCategory.BINARY) {
            throw new UDFArgumentException("hll_get_precision takes a binary object which was created with the hyperloglog UDAF");
        }
        this.binaryInspector = (BinaryObjectInspector) primInsp;

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }


}

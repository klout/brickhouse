package brickhouse.udf.json;
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

import brickhouse.udf.json.InspectorHandle.InspectorHandleFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Given a JSON String containing a map with values of all the same type,
 * return a Hive map of key-value pairs
 */

@Description(name = "json_map",
        value = "_FUNC_(json) - Returns a map of key-value pairs from a JSON string"
)
public class JsonMapUDF extends GenericUDF {
    private StringObjectInspector stringInspector;
    private InspectorHandle inspHandle;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 1 && arguments.length != 2) {
            throw new UDFArgumentException("Usage : json_map( jsonstring, optional typestring ) ");
        }
        if (!arguments[0].getCategory().equals(Category.PRIMITIVE)
                || ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
            throw new UDFArgumentException("Usage : json_map( jsonstring, optional typestring) ");
        }

        stringInspector = (StringObjectInspector) arguments[0];

        if (arguments.length > 1) {
            if (!arguments[1].getCategory().equals(Category.PRIMITIVE)
                    && ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
                throw new UDFArgumentException("Usage : json_map( jsonstring, optional typestring) ");
            }
            if (!(arguments[1] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("json_map( jsonstring, typestring ) : typestring must be a constant");
            }
            ConstantObjectInspector constInsp = (ConstantObjectInspector) arguments[1];
            String typeStr = ((Text) constInsp.getWritableConstantValue()).toString();

            String[] types = typeStr.split(",");
            if (types.length != 2) {
                throw new UDFArgumentException(" typestring must be of the form <keytype>,<valuetype>");
            }
            TypeInfo keyType = TypeInfoUtils.getTypeInfoFromTypeString(types[0]);
            TypeInfo valType = TypeInfoUtils.getTypeInfoFromTypeString(types[1]);

            ObjectInspector keyInsp = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(keyType);
            ObjectInspector valInsp = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(valType);

            MapObjectInspector mapInsp = ObjectInspectorFactory.getStandardMapObjectInspector(keyInsp, valInsp);

            inspHandle = InspectorHandleFactory.GenerateInspectorHandle(mapInsp);

            return inspHandle.getReturnType();

        } else {
            ObjectInspector keyInsp = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            ObjectInspector valueInsp = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector; /// XXX Make value type configurable somehow

            MapObjectInspector mapInsp = ObjectInspectorFactory.getStandardMapObjectInspector(keyInsp, valueInsp);

            inspHandle = InspectorHandleFactory.GenerateInspectorHandle(mapInsp);

            return inspHandle.getReturnType();
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            String jsonString = this.stringInspector.getPrimitiveJavaObject(arguments[0].get());

            //// Logic is the same as "from_json"
            ObjectMapper om = new ObjectMapper();
            JsonNode jsonNode = om.readTree(jsonString);
            return inspHandle.parseJson(jsonNode);

        } catch (JsonProcessingException e) {
            throw new HiveException(e);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "json_map( " + children[0] + " )";
    }

}

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

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

@Description(name="json_to_array",
    value = "_FUNC_(jsonstring) - Returns a array of JSON string from a JSON array."
)
public class JsonToArrayUDF extends GenericUDF {
  StringObjectInspector stringInspector;

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    ArrayList<String> anArray = new ArrayList<String>();
    try {
      String jsonString = this.stringInspector.getPrimitiveJavaObject(args[0].get());

      if(jsonString.equalsIgnoreCase("null"))
        return null;

      if(jsonString.equalsIgnoreCase("[]"))
        return anArray;

      if(!jsonString.startsWith("[") || !jsonString.endsWith("]")) {
        return null;
      }

      jsonString = jsonString.substring(1, jsonString.length()-1);
      
      int contInternos = 0;
      for(int i = 0; i < jsonString.length(); i++) {
        if(jsonString.charAt(i) == '{') {
          for(int j = i+1; j < jsonString.length(); j++) {
            if(jsonString.charAt(j) == '{') {
              contInternos++;
            }
            if(jsonString.charAt(j) == '}' && contInternos == 0) {
               anArray.add(jsonString.substring(i+1, j));
               i = j;
               break;
            } else if(jsonString.charAt(j) == '}') {
              contInternos--;
            }
          }
        }
      }

      return anArray;
    //} catch (IOException e) {
    //  throw new HiveException(e);
    } catch (NullPointerException npe){
      return null;
    }
  }

  @Override
  public String getDisplayString(String[] args) {
    return "json_to_array (jsonstring)";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {

        if(args.length != 1) {
         throw new UDFArgumentException("Usage : json_to_array (jsonstring) ");
       }

        if(((PrimitiveObjectInspector)args[0]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
         throw new UDFArgumentException("Usage : json_to_array (jsonstring) ");
       }

     stringInspector = (StringObjectInspector) args[0];

    ObjectInspector valInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    return ObjectInspectorFactory.getStandardListObjectInspector(valInspector);
  }

}

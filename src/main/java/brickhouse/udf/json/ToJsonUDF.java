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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
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

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Generate a JSON string from an arbitrary Hive structure.
 * Use the struct() method to generate arbitrary JSON maps.
 * <p/>
 * <p>For example,
 * <p/>
 * to_json( struct("name":"Bob",
 * "value",23.0,
 * "color_list",array( "red", "yellow", "green" ),
 * "inner_map", map( "a", 1, "b", 2, "c" 3) ) )
 * = '{ "name":"Bob", "value":23.0,
 * "color_list":[ "red", "yellow", "green" ],
 * "inner_map":{ "a":1, "b":2, "c":3 } }'
 */
@Description(name = "to_json",
        value = "_FUNC_(struct, convert_to_camel_case) - Returns a JSON string from an arbitrary Hive structure."
)
public class ToJsonUDF extends GenericUDF {
    private InspectorHandle inspHandle;
    private Boolean convertFlag = Boolean.FALSE;
    private JsonFactory jsonFactory;


    private interface InspectorHandle {
        abstract public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException;
    }

    ;


    private class MapInspectorHandle implements InspectorHandle {
        private MapObjectInspector mapInspector;
        private StringObjectInspector keyObjectInspector;
        private InspectorHandle valueInspector;


        public MapInspectorHandle(MapObjectInspector mInsp) throws UDFArgumentException {
            mapInspector = mInsp;
            try {
                keyObjectInspector = (StringObjectInspector) mInsp.getMapKeyObjectInspector();
            } catch (ClassCastException castExc) {
                throw new UDFArgumentException("Only Maps with strings as keys can be converted to valid JSON");
            }
            valueInspector = GenerateInspectorHandle(mInsp.getMapValueObjectInspector());
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                Map map = mapInspector.getMap(obj);
                Iterator<Map.Entry> iter = map.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = iter.next();
                    String keyJson = keyObjectInspector.getPrimitiveJavaObject(entry.getKey());
                    if (convertFlag) {
                        gen.writeFieldName(FromJsonUDF.ToCamelCase(keyJson));
                    } else {
                        gen.writeFieldName(keyJson);
                    }
                    valueInspector.generateJson(gen, entry.getValue());
                }
                gen.writeEndObject();
            }
        }

    }


    private class StructInspectorHandle implements InspectorHandle {
        private StructObjectInspector structInspector;
        private List<String> fieldNames;
        private List<InspectorHandle> fieldInspectorHandles;

        public StructInspectorHandle(StructObjectInspector insp) throws UDFArgumentException {
            structInspector = insp;
            List<? extends StructField> fieldList = insp.getAllStructFieldRefs();
            this.fieldNames = new ArrayList<String>();
            this.fieldInspectorHandles = new ArrayList<InspectorHandle>();
            for (StructField sf : fieldList) {
                fieldNames.add(sf.getFieldName());
                fieldInspectorHandles.add(GenerateInspectorHandle(sf.getFieldObjectInspector()));
            }
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            //// Interpret a struct as a map ...
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                List structObjs = structInspector.getStructFieldsDataAsList(obj);

                for (int i = 0; i < fieldNames.size(); ++i) {
                    String fieldName = fieldNames.get(i);
                    if (convertFlag) {
                        gen.writeFieldName(FromJsonUDF.ToCamelCase(fieldName));
                    } else {
                        gen.writeFieldName(fieldName);
                    }
                    fieldInspectorHandles.get(i).generateJson(gen, structObjs.get(i));
                }
                gen.writeEndObject();
            }
        }

    }


    private class ArrayInspectorHandle implements InspectorHandle {
        private ListObjectInspector arrayInspector;
        private InspectorHandle valueInspector;


        public ArrayInspectorHandle(ListObjectInspector lInsp) throws UDFArgumentException {
            arrayInspector = lInsp;
            valueInspector = GenerateInspectorHandle(arrayInspector.getListElementObjectInspector());
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartArray();
                List list = arrayInspector.getList(obj);
                for (Object listObj : list) {
                    valueInspector.generateJson(gen, listObj);
                }
                gen.writeEndArray();
            }
        }

    }

    private class StringInspectorHandle implements InspectorHandle {
        private StringObjectInspector strInspector;


        public StringInspectorHandle(StringObjectInspector insp) {
            strInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                String str = strInspector.getPrimitiveJavaObject(obj);
                gen.writeString(str);
            }
        }

    }

    private class IntInspectorHandle implements InspectorHandle {
        private IntObjectInspector intInspector;

        public IntInspectorHandle(IntObjectInspector insp) {
            intInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null)
                gen.writeNull();
            else {
                int num = intInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class DoubleInspectorHandle implements InspectorHandle {
        private DoubleObjectInspector dblInspector;

        public DoubleInspectorHandle(DoubleObjectInspector insp) {
            dblInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                double num = dblInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class LongInspectorHandle implements InspectorHandle {
        private LongObjectInspector longInspector;

        public LongInspectorHandle(LongObjectInspector insp) {
            longInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                long num = longInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class ShortInspectorHandle implements InspectorHandle {
        private ShortObjectInspector shortInspector;

        public ShortInspectorHandle(ShortObjectInspector insp) {
            shortInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                short num = shortInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }


    private class ByteInspectorHandle implements InspectorHandle {
        private ByteObjectInspector byteInspector;

        public ByteInspectorHandle(ByteObjectInspector insp) {
            byteInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                byte num = byteInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }


    private class FloatInspectorHandle implements InspectorHandle {
        private FloatObjectInspector floatInspector;

        public FloatInspectorHandle(FloatObjectInspector insp) {
            floatInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                float num = floatInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class BooleanInspectorHandle implements InspectorHandle {
        private BooleanObjectInspector boolInspector;

        public BooleanInspectorHandle(BooleanObjectInspector insp) {
            boolInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                boolean tf = boolInspector.get(obj);
                gen.writeBoolean(tf);
            }
        }
    }

    private class BinaryInspectorHandle implements InspectorHandle {
        private BinaryObjectInspector binaryInspector;

        public BinaryInspectorHandle(BinaryObjectInspector insp) {
            binaryInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                byte[] bytes = binaryInspector.getPrimitiveJavaObject(obj);
                gen.writeBinary(bytes);
            }
        }
    }

    private class TimestampInspectorHandle implements InspectorHandle {
        private TimestampObjectInspector timestampInspector;
        private DateTimeFormatter isoFormatter = ISODateTimeFormat.dateTimeNoMillis();

        public TimestampInspectorHandle(TimestampObjectInspector insp) {
            timestampInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                Timestamp timestamp = timestampInspector.getPrimitiveJavaObject(obj);
                String timeStr = isoFormatter.print(timestamp.getTime());
                gen.writeString(timeStr);
            }
        }
    }


    private InspectorHandle GenerateInspectorHandle(ObjectInspector insp) throws UDFArgumentException {
        Category cat = insp.getCategory();
        if (cat == Category.MAP) {
            return new MapInspectorHandle((MapObjectInspector) insp);
        } else if (cat == Category.LIST) {
            return new ArrayInspectorHandle((ListObjectInspector) insp);
        } else if (cat == Category.STRUCT) {
            return new StructInspectorHandle((StructObjectInspector) insp);
        } else if (cat == Category.PRIMITIVE) {
            PrimitiveObjectInspector primInsp = (PrimitiveObjectInspector) insp;
            PrimitiveCategory primCat = primInsp.getPrimitiveCategory();
            if (primCat == PrimitiveCategory.STRING) {
                return new StringInspectorHandle((StringObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.INT) {
                return new IntInspectorHandle((IntObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.LONG) {
                return new LongInspectorHandle((LongObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.SHORT) {
                return new ShortInspectorHandle((ShortObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.BOOLEAN) {
                return new BooleanInspectorHandle((BooleanObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.FLOAT) {
                return new FloatInspectorHandle((FloatObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.DOUBLE) {
                return new DoubleInspectorHandle((DoubleObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.BYTE) {
                return new ByteInspectorHandle((ByteObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.BINARY) {
                return new BinaryInspectorHandle((BinaryObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.TIMESTAMP) {
                return new TimestampInspectorHandle((TimestampObjectInspector) primInsp);
            }


        }
        /// Dunno ...
        throw new UDFArgumentException("Don't know how to handle object inspector " + insp);
    }


    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator gen = jsonFactory.createJsonGenerator(writer);
            inspHandle.generateJson(gen, args[0].get());
            gen.close();
            writer.close();
            return writer.toString();
        } catch (IOException io) {
            throw new HiveException(io);
        }

    }

    @Override
    public String getDisplayString(String[] args) {
        return "to_json(" + args[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 1 && args.length != 2) {
            throw new UDFArgumentException(" ToJson takes an object as an argument, and an optional to_camel_case flag");
        }
        ObjectInspector oi = args[0];
        inspHandle = GenerateInspectorHandle(oi);

        if (args.length == 2) {
            ObjectInspector flagInsp = args[1];
            if (flagInsp.getCategory() != Category.PRIMITIVE
                    || ((PrimitiveObjectInspector) flagInsp).getPrimitiveCategory()
                    != PrimitiveCategory.BOOLEAN
                    || !(flagInsp instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException(" ToJson takes an object as an argument, and an optional to_camel_case flag");
            }
            WritableConstantBooleanObjectInspector constInsp = (WritableConstantBooleanObjectInspector) flagInsp;
            convertFlag = constInsp.getWritableConstantValue().get();
        }

        jsonFactory = new JsonFactory();

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

}

package brickhouse.udf.json;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.codehaus.jackson.JsonNode;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


public interface InspectorHandle {
	
	Object parseJson(JsonNode jsonNode);
    ObjectInspector getReturnType();	
    
    final public class InspectorHandleFactory {
    	static public InspectorHandle GenerateInspectorHandle( ObjectInspector insp ) throws UDFArgumentException {
    		Category cat = insp.getCategory();
    		switch( cat)  {
    		case LIST:
    			return new InspectorHandle.ListHandle( (ListObjectInspector)insp );
    		case MAP:
    			return new InspectorHandle.MapHandle( (MapObjectInspector)insp);
    		case STRUCT:
    			return new InspectorHandle.StructHandle( (StructObjectInspector)insp);
    		case PRIMITIVE:
    			return new InspectorHandle.PrimitiveHandle( (PrimitiveObjectInspector)insp);
    		}
    		return null;
    	}
    	
    	static public InspectorHandle GenerateInspectorHandleFromTypeInfo( String typeStr ) throws UDFArgumentException {
    	    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    	    ObjectInspector objInsp = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    	    return GenerateInspectorHandle(objInsp);
    	}
    }
    /** 
     * If one passes a named-struct in, then one can parse arbitrary
     *   structures
     **/
    class StructHandle implements InspectorHandle {
    	/**
    	 * 
    	 */
    	private List<String> fieldNames;
    	private List<InspectorHandle> handleList;
    	
    	
    	public StructHandle( StructObjectInspector structInspector) throws UDFArgumentException {
    		fieldNames = new ArrayList<String>();
    		handleList = new ArrayList<InspectorHandle>();
    		
    		List<? extends StructField> refs =  structInspector.getAllStructFieldRefs();
    		for( StructField ref : refs) {
    			fieldNames.add( ref.getFieldName());
    			InspectorHandle fieldHandle = InspectorHandleFactory.GenerateInspectorHandle( ref.getFieldObjectInspector() );
    			handleList.add( fieldHandle);
    		}
    	}

    	@Override
    	public Object parseJson(JsonNode jsonNode) {
    		/// For structs, they just return a list of object values
    		if(jsonNode.isNull())
    			return null;
    		List<Object> valList = new ArrayList<Object>();
    		
    		for(int i=0; i< fieldNames.size(); ++i) {
    			String key = fieldNames.get( i);
    			JsonNode valNode = jsonNode.get( key);
    			InspectorHandle valHandle = handleList.get(i);
    			
    			Object valObj = valHandle.parseJson(valNode);
    			valList.add( valObj);
    		}
    		
    		return valList;
    	}

    	@Override
    	public ObjectInspector getReturnType() {
    		List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    		for( InspectorHandle fieldHandle : handleList) {
    			structFieldObjectInspectors.add( fieldHandle.getReturnType() );
    		}
    		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, structFieldObjectInspectors);
    	}
    	
    }
    
	
	class MapHandle implements InspectorHandle {
		private InspectorHandle mapValHandle;
		private StandardMapObjectInspector retInspector;

		/// for JSON maps (or "objects"), the keys are always string objects
		///  
		public MapHandle( MapObjectInspector insp) throws UDFArgumentException {
			if( !(insp.getMapKeyObjectInspector() instanceof StringObjectInspector)) {
				throw new RuntimeException( " JSON maps can only have strings as keys");
			}
			mapValHandle = InspectorHandleFactory.GenerateInspectorHandle( insp.getMapValueObjectInspector() );
		}
		@Override
		public Object parseJson(JsonNode jsonNode) {
			if(jsonNode.isNull()) 
				return null;
			Map<String,Object> newMap = (Map<String,Object>)retInspector.create();
			
			Iterator<String> keys = jsonNode.getFieldNames();
			while( keys.hasNext()) {
				String key = keys.next();
				JsonNode valNode = jsonNode.get( key);
				Object val = mapValHandle.parseJson(valNode);
				newMap.put( key, val);
			}
			return newMap;
		}

		@Override
		public ObjectInspector getReturnType() {
			retInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
					PrimitiveObjectInspectorFactory.javaStringObjectInspector,
					mapValHandle.getReturnType() );
			return retInspector;
		}
		
	}
	
	public class ListHandle implements InspectorHandle {
		private StandardListObjectInspector retInspector;
		private InspectorHandle elemHandle;

		public ListHandle( ListObjectInspector insp) throws UDFArgumentException {
			elemHandle = InspectorHandleFactory.GenerateInspectorHandle( insp.getListElementObjectInspector() );
		}
		
		@Override
		public Object parseJson(JsonNode jsonNode) {
			if(jsonNode.isNull() )
				return null;
			List newList = (List) retInspector.create(0);
			
			Iterator<JsonNode> listNodes = jsonNode.getElements();
			while(listNodes.hasNext()) {
				JsonNode elemNode = listNodes.next();
				if( elemNode != null) {
					Object elemObj = elemHandle.parseJson(elemNode);
					newList.add( elemObj);
				} else {
					newList.add(null);
				}
			}
			return newList;
		}

		@Override
		public ObjectInspector getReturnType() {
			retInspector =  ObjectInspectorFactory.getStandardListObjectInspector( elemHandle.getReturnType() );
			return retInspector;
		}
		
	}
	

	class PrimitiveHandle implements InspectorHandle {
		private PrimitiveCategory category;
        private DateTimeFormatter isoFormatter = ISODateTimeFormat.dateTimeNoMillis();

		
		public PrimitiveHandle(PrimitiveObjectInspector insp) throws UDFArgumentException {
			category = insp.getPrimitiveCategory();
			
		}

		@Override
		public Object parseJson(JsonNode jsonNode) {
			if(jsonNode == null
					|| jsonNode.isNull()) {
				return null;
			}
			switch( category) {
			case STRING:
				return jsonNode.toString();
			case LONG:
				return jsonNode.getLongValue();
			case SHORT:
				return (short)jsonNode.getIntValue();
			case BYTE:
				return (byte)jsonNode.getIntValue();
			case BINARY:
				try {
				  return jsonNode.getBinaryValue();
				} catch(IOException ioExc) {
					return jsonNode.toString();
				}
			case INT:
				return jsonNode.getIntValue();
			case FLOAT:
				return new Float(jsonNode.getDoubleValue());
			case DOUBLE:
				return jsonNode.getDoubleValue();
			case BOOLEAN:
				return jsonNode.getBooleanValue();
			case TIMESTAMP:
			    long time = isoFormatter.parseMillis( jsonNode.getTextValue());
				return new Timestamp(time);
			}
			return null;
		}

		@Override
		public ObjectInspector getReturnType() {
			return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(category);
		}

		
	}
	
}

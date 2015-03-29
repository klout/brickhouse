package brickhouse.udf.dcache;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 *   UDF to access a distributed map file 
 *   
 *   Assumes the file is a tab-separated file of name-value pairs,
 *   which has been placed in distributed cache using the "add file" command
 * 
 * Example 
 * 
 *  INSERT OVERWRITE LOCAL DIRECTORY mymap select key,value from my_map_table;
 *  ADD FILE mymap;
 *  
 *  select key, val* distributed_map( key, 'mymap') from the_table;
 *   
 *   
 *   If one argument is passed in, it is assumed to be a filename, containing
 *    a map of type map&lt;string,double&gt;, and the entire map is returned.
 *    
 *   If two arguments are passed in, it is either filename, and a string specifying the
 *    type of the map ( i.e distributed_map('mymap','map&lt;string,bigint&gt;'); ) and returns
 *     the entire map, or it is the key and the filename ( ie distributed_map( key, 'mymap'),
 *     and only the key's value is returned.
 *     
 *   If there are three arguments passed in, it is assumed to be the key, the filename, and the 
 *    maptype, (i.e distributed_map( key, 'mymap', 'map&lt;string,bigint&gt;') )
 *
 */
@UDFType(deterministic=false)
public class DistributedMapUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(DistributedMapUDF.class);
	private static HashMap<String,HashMap<Object,Object>> localMapMap = new HashMap<String,HashMap<Object,Object>>();
	private StringObjectInspector fileNameInspector;
	private PrimitiveObjectInspector keyInspector;
	private TypeInfo keyType;
	private TypeInfo valType;
	private LazySimpleSerDe serde;


	private LazySimpleSerDe getLineSerde() throws SerDeException {
		if(serde == null) {
			Logger.getLogger(LazySimpleSerDe.class).setLevel(Level.DEBUG);
			serde = new LazySimpleSerDe();
			Configuration job = new Configuration();
			Properties tbl =new Properties();
			tbl.setProperty("columns", "key,value");
			tbl.setProperty("columns.types", keyType.getTypeName() +"," + valType.getTypeName());
			serde.initialize(job, tbl);
		}
		return serde;
		
	}
	
	private void addValues(HashMap<Object,Object> map, String mapFilename) throws IOException, SerDeException {
		if(!mapFilename.endsWith("crc")) {
			File mapFile  = new File( mapFilename);
			if( mapFile.isDirectory() ) {
				String[] subFiles = mapFile.list();
				for(String subFile : subFiles) {
					LOG.info( "Checking recursively " + subFile);
					addValues( map, mapFilename + "/" +subFile);
				}
			} else {
				BufferedReader reader = new BufferedReader(new InputStreamReader( new FileInputStream( mapFile)));
				
				SerDe lazy =  getLineSerde();
				StructObjectInspector lineInsp = (StructObjectInspector) lazy.getObjectInspector();
				StructField keyRef = lineInsp.getStructFieldRef("key");
				StructField valueRef = lineInsp.getStructFieldRef("value");
				
				

				String line;
				while( (line = reader.readLine() ) != null ) {
					Writable lineText = new Text( line);
					Object lineObj = lazy.deserialize(lineText);
					List<Object> objList = lineInsp.getStructFieldsDataAsList(lineObj);
					Object key = ((PrimitiveObjectInspector)keyRef.getFieldObjectInspector()).getPrimitiveJavaObject(objList.get(0));
					Object val = ((PrimitiveObjectInspector)valueRef.getFieldObjectInspector()).getPrimitiveJavaObject(objList.get(1));
					map.put(key,val);
				}
			}
		} else {
			LOG.info(" Ignoring CRC file " + mapFilename);
		}
	}
	

	private Map<Object,Object> getLocalMap( String mapFileName) {
		HashMap<Object,Object> map = localMapMap.get( mapFileName);
		if( map == null ) {
			try {
				File localDir = new File(".");
				String[] files = localDir.list();
				for( String file : files) {
					LOG.info(" In current dir is " + file);
					File checkFile = new File(file);
					if(checkFile.isDirectory() ) {
						LOG.info(" FILE " + file + " is a directory");
					}
				}
				map = new HashMap<Object,Object>();
				addValues( map,mapFileName);
				
				localMapMap.put( mapFileName, map);
			} catch(IOException ioExc) {
			ioExc.printStackTrace();
			throw new RuntimeException(ioExc);
			
		    } catch (SerDeException serdeExc) {
			  throw new RuntimeException(serdeExc);
			}
		}
		return map;
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		//// if keyInspector has been set 
		if( this.keyInspector != null) {
			Object key = keyInspector.getPrimitiveJavaObject(arg0[0].get());
			String mapFileName = this.fileNameInspector.getPrimitiveJavaObject(arg0[1].get());
			Map<Object,Object> map = getLocalMap( mapFileName);
			return map.get(key);
		} else {
		   Object mapFNameObj;
		   if( arg0.length == 1) {
		     mapFNameObj = arg0[0].get();
		   } else {
			 mapFNameObj = arg0[1].get();
		   }
		   String mapFileName = this.fileNameInspector.getPrimitiveJavaObject(mapFNameObj);
		   Map<Object,Object> map = getLocalMap( mapFileName);
		   return map;
		}
	}


	@Override
	public String getDisplayString(String[] arg0) {
		return "distributed_map()";
	}

	public String usage(String err) {
		return " Distributed Map -- Case  " + err;
	}
	
	private MapObjectInspector getMapType( String typeStr) throws UDFArgumentException,IllegalArgumentException {
		try {
			TypeInfo hiveType = TypeInfoUtils.getTypeInfoFromTypeString( typeStr);
			if( hiveType.getCategory() != Category.MAP) {
				throw new UDFArgumentException(usage("Type is not map"));
			}
			MapObjectInspector mapInsp =  (MapObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(hiveType);
			if(mapInsp.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE) {
				throw new UDFArgumentException(usage("Key is not primitive"));
			}

			return mapInsp;
		} catch(IllegalArgumentException badTypeStr) {
			throw new UDFArgumentException(usage("String is not type"));
		}
	}
	
	/**
	 *  Either one, two or three values can be passed in.
	 *  If one argument is passed it, it is implied that the 
	 *   return value is a map&lt;string,double&gt;. If three arguments
	 *   are passed in, then it is implied the arguments are the
	 *    map key, the map filename, and the value type.
	 *    
	 *    If two arguments are passed in, it is implied that either)
	 *     a map key, and a filename are being passed in, 
	 *    or a filename, and a map return type are being passed in.
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if( arg0.length == 0 || arg0.length > 3)
			throw new UDFArgumentException( usage("Between 1 and 3 arguments"));
		switch(arg0.length) {
		case 1:
			//// filename
			if( !( arg0[0] instanceof ConstantObjectInspector) 
				|| !( arg0[0] instanceof StringObjectInspector) ) {
			   throw new UDFArgumentException( usage(" 1 arguments is always name of directory"));
			}
			fileNameInspector = (StringObjectInspector) arg0[0];
			keyType = TypeInfoFactory.stringTypeInfo;
			valType = TypeInfoFactory.doubleTypeInfo;
			return ObjectInspectorFactory.getStandardMapObjectInspector( PrimitiveObjectInspectorFactory.javaStringObjectInspector,
					PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
		 
		case 2:
			//// either key, filename ... 
			///  or filename, maptype
			if( !(arg0[1] instanceof ConstantObjectInspector)  
				|| !(arg0[1] instanceof StringObjectInspector)
				|| !( arg0[0] instanceof PrimitiveObjectInspector)) {
				throw new UDFArgumentException( usage("2 arguments is eiter key and filename, or a filename and maptype"));
			}
	        ConstantObjectInspector mapType = (ConstantObjectInspector)arg0[1];
			String typeStr = mapType.getWritableConstantValue().toString();
			try {
				//// able to parse map type ...
				MapObjectInspector mapInsp = getMapType( typeStr);
				keyType =  TypeInfoFactory.getPrimitiveTypeInfo( mapInsp.getMapKeyObjectInspector().getTypeName());
				valType =  TypeInfoFactory.getPrimitiveTypeInfo( mapInsp.getMapValueObjectInspector().getTypeName());
				fileNameInspector = (StringObjectInspector) arg0[0];
			    return ObjectInspectorUtils.getStandardObjectInspector(mapInsp);
				
			} catch(UDFArgumentException checkMapType ) {
				/// Assume that it is key, filename 
				this.keyInspector = (PrimitiveObjectInspector) arg0[0];
				keyType =  TypeInfoFactory.getPrimitiveTypeInfo( keyInspector.getTypeName());
				valType =  TypeInfoFactory.doubleTypeInfo;
				this.fileNameInspector = (StringObjectInspector) arg0[1];
				//// Default case is  string, double
				return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
			}
		case 3:
			//// key , filename , maptype
			if( !( arg0[1] instanceof ConstantObjectInspector) 
				|| !( arg0[1] instanceof StringObjectInspector)  
			    || !(arg0[2] instanceof ConstantObjectInspector) 
				|| !( arg0[2] instanceof StringObjectInspector) 
				|| !( arg0[0] instanceof PrimitiveObjectInspector)) {
				throw new UDFArgumentException( usage("3 arguments are key,filename and maptype"));
			}
			fileNameInspector = (StringObjectInspector) arg0[1];
			
			ConstantObjectInspector mapType3 = (ConstantObjectInspector)arg0[2];
			String typeStr3 = mapType3.getWritableConstantValue().toString();
			MapObjectInspector mapInspect = this.getMapType(typeStr3);
			
			keyInspector = (PrimitiveObjectInspector) arg0[0];
			if(keyInspector.getPrimitiveCategory() !=
				((PrimitiveObjectInspector)mapInspect.getMapKeyObjectInspector() ).getPrimitiveCategory() ) {
				throw new UDFArgumentException( usage("Key must be primitive"));
			}
			
			keyType =  TypeInfoFactory.getPrimitiveTypeInfo( keyInspector.getTypeName());
			
			ObjectInspector valInspector = ObjectInspectorUtils.getStandardObjectInspector(mapInspect.getMapValueObjectInspector());
			/// XXX Can we have non primitives for the values ????
			valType = TypeInfoFactory.getPrimitiveTypeInfo(valInspector.getTypeName());
			return valInspector;
		}
		return null;
	}

}

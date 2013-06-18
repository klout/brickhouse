package brickhouse.udf.collect;
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

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;


/**
 *   Workaround for the Hive bug 
 *   https://issues.apache.org/jira/browse/HIVE-1955
 *   
 *  FAILED: Error in semantic analysis: Line 4:3 Non-constant expressions for array indexes not supported key
 *  
 *   
 *  Use instead of [ ] syntax,   
 *
 *
 */
public class ArrayIndexUDF extends GenericUDF {
	private ListObjectInspector listInspector;
	private ObjectInspector elemInspector;
	private IntObjectInspector intInspector;
	

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		Object list =  arg0[0].get();
		int idx = intInspector.get( arg0[1].get() );
		
		if (idx < 0) {
			idx = listInspector.getListLength( arg0[0] ) + idx;
		}

		Object unInsp =  listInspector.getListElement(list, idx);
				
		return unInsp;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "array_index( " + arg0[0] + " , " + arg0[1]  + " )";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if( arg0.length != 2) {
			throw new UDFArgumentException("array_index takes an array and an int as arguments");
		}
		if( arg0[0].getCategory() != Category.LIST 
			|| arg0[1].getCategory() != Category.PRIMITIVE 
			|| ((PrimitiveObjectInspector)arg0[1]).getPrimitiveCategory() != PrimitiveCategory.INT) {
			throw new UDFArgumentException("array_index takes an array and an int as arguments");
		}
		listInspector = (ListObjectInspector) arg0[0];
		intInspector = (IntObjectInspector) arg0[1];
		
		return  listInspector.getListElementObjectInspector();
	}

}

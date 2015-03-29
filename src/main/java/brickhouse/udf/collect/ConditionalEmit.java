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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * ConditionalEmit takes an array of booleans and strings, 
 *  and emits records if the boolean is true. 
 *  
 *  This allows you to emit multiple rows on one pass of the data,
 *   rather than doing a union of multiple views with different where clauses.
 *   
 *   select 
 *       conditional_emit( array( maxwell_score &gt; 80 , 
 *                     abs( maxwell_score - other.maxwell_score ) &lt; 5,
 *                     city = "New York" ),
 *                       array( "CELEB" , "PEER", "NEW_YORKER" ) )
 *    from big_table_which_is_hard_to_sort;
 *     
 *     
 *
 */
@Description(name = "conditional_emit",
value = "_FUNC_(a,b) - Emit features of a row according to various conditions ")
public class ConditionalEmit extends GenericUDTF {
	private ListObjectInspector conditionInspector = null;
	private ListObjectInspector featureInspector = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		if( argOIs.length != 2 ) {
			throw new UDFArgumentException("conditional_emit  takes 2 arguments, array<boolean>, array<string>");
		}
		if( argOIs[0].getCategory() != Category.LIST ) {
			throw new UDFArgumentException("conditional_emit  takes 2 arguments, array<boolean>, array<string>");
		}
		conditionInspector = (ListObjectInspector) argOIs[0];
		if( conditionInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
			throw new UDFArgumentException("conditional_emit  takes 2 arguments, array<boolean>, array<string>");
		}
		PrimitiveObjectInspector boolInspector = (PrimitiveObjectInspector) conditionInspector.getListElementObjectInspector();
		if( boolInspector.getPrimitiveCategory() != PrimitiveCategory.BOOLEAN) {
			throw new UDFArgumentException("conditional_emit  takes 2 arguments, array<boolean>, array<string>");
		}
		featureInspector = (ListObjectInspector) argOIs[1];
		if( featureInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
			throw new UDFArgumentException("conditional_emit  takes 2 arguments, array<boolean>, array<string>");
		}
		PrimitiveObjectInspector strInspector = (PrimitiveObjectInspector) featureInspector.getListElementObjectInspector();
		if( strInspector.getPrimitiveCategory() != PrimitiveCategory.STRING) {
			throw new UDFArgumentException("conditional_emit  takes 2 arguments, array<boolean>, array<string>");
		}
		
	    ArrayList<String> fieldNames = new ArrayList<String>();
	    fieldNames.add("feature");
	    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	    fieldOIs.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    
	    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
	        fieldOIs);
	}

    private final Object[] forwardListObj = new Object[1];
    
	@Override
	public void process(Object[] args) throws HiveException {
		List conditionList = this.conditionInspector.getList( args[0]);
		List featureList = this.featureInspector.getList( args[1]);
		
		if( conditionList.size() != featureList.size() ) {
			throw new HiveException(" condition_emit, lists must be of same length");
		}
		
		for( int i=0; i<conditionList.size(); ++i) {
			BooleanObjectInspector predInspector = (BooleanObjectInspector)this.conditionInspector.getListElementObjectInspector();
			Object checkGet = conditionList.get( i);
			if( checkGet != null) {
				Boolean predicate =  predInspector.get(checkGet);
				if(predicate.booleanValue() == true) {
					StringObjectInspector featureNameInspector = (StringObjectInspector)this.featureInspector.getListElementObjectInspector();
					String featureName = featureNameInspector.getPrimitiveJavaObject( featureList.get(i));
					forwardListObj[0] = featureName;
					forward( forwardListObj);
				}
			}
		}
		
		
	}

	@Override
	public void close() throws HiveException {
		
	}

}

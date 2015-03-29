package brickhouse.udf.sketch;
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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import brickhouse.analytics.uniques.SketchSet;

/**
 *  UDF to combine two sketch sets, to estimate size of set union.
 *  
 *  Sketch sets can be either the set of original strings or the  
 *    MD5 hashes. If array&lt;string&gt; is passed in, it is assumed to be 
 *    the original sketch_set values; if array&lt;bigint&gt; is used, then
 *     it is assumed to be the KMin hash values created with sketch_values
 *
 */
@Description(name="combine_sketch",
    value = "_FUNC_(x) - Combine two sketch sets. "
)
public class CombineSketchUDF extends GenericUDF {
	private ListObjectInspector listInspectors[];
	private PrimitiveCategory elemCategory;
	private int sketchSetSize = SketchSetUDAF.DEFAULT_SKETCH_SET_SIZE;
	
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		SketchSet ss = new SketchSet(sketchSetSize);
		for( int i=0; i< arg0.length; ++i) {
			Object listObj = arg0[i].get();
			int listLen = listInspectors[i].getListLength(listObj);
			for(int j=0; j< listLen; ++j ) {
		       Object uninspObj = listInspectors[i].getListElement(listObj, j);
		       switch( elemCategory) {
		       case STRING:
		    	   StringObjectInspector strInspector = (StringObjectInspector) listInspectors[i].getListElementObjectInspector();
		    	   String item = strInspector.getPrimitiveJavaObject(uninspObj);
		    	   ss.addItem(item);
		    	   break;
		       case LONG: 
		    	   LongObjectInspector bigintInspector = (LongObjectInspector) listInspectors[i].getListElementObjectInspector();
		    	   long itemHash = bigintInspector.get(uninspObj);
		    	   ss.addHash( itemHash);
		    	   break;
		       }
			}
		}
	    switch( elemCategory) {
	      case STRING:
	    	  return ss.getMinHashItems();
	      case LONG:
	    	  return ss.getMinHashes();
	      default:
	    	  /// will never happen
	    	  throw new HiveException("Unexpected Element Category " + elemCategory);
	    }
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "combine_sketch";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if( arg0.length < 2 ) {
			throw new UDFArgumentException("combine_sketch takes at least two arguments; a set of array<string> or a set of array<bigint>");
		}
		if(arg0[0].getCategory() != Category.LIST) {
			throw new UDFArgumentException("combine_sketch takes at least two arguments; a set of array<string> or a set of array<bigint>");
		}
		ObjectInspector lastInspector = arg0[arg0.length -1 ];
		int listLen = arg0.length;
		if( lastInspector.getCategory() == Category.PRIMITIVE 
		        && ((PrimitiveObjectInspector)lastInspector).getPrimitiveCategory() == PrimitiveCategory.INT) {
		   if(lastInspector instanceof ConstantObjectInspector)  {
		       
		   } else {
		      throw new UDFArgumentException(" Sketch set size must an integer");
		   }
		       
		}
		this.listInspectors = new ListObjectInspector[ arg0.length];
		this.listInspectors[0] = (ListObjectInspector) arg0[0];
		if( this.listInspectors[0].getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
			throw new UDFArgumentException("combine_sketch takes at least two arguments; a set of array<string> or a set of array<bigint>");
		}
		this.elemCategory = ((PrimitiveObjectInspector)((listInspectors[0].getListElementObjectInspector()))).getPrimitiveCategory();
		if(this.elemCategory != PrimitiveCategory.STRING && this.elemCategory != PrimitiveCategory.LONG) {
			throw new UDFArgumentException("combine_sketch takes at least two arguments; a set of array<string> or a set of array<bigint>");
		}
		for(int i=1; i< arg0.length; ++i) {
			if( arg0[i].getCategory() != Category.LIST) {
			   throw new UDFArgumentException("combine_sketch takes at least two arguments; a set of array<string> or a set of array<bigint>");
			}
			this.listInspectors[i] = (ListObjectInspector) arg0[i];
			if(((PrimitiveObjectInspector)((listInspectors[0].getListElementObjectInspector()))).getPrimitiveCategory() != elemCategory ) {
			   throw new UDFArgumentException("combine_sketch takes at least two arguments; a set of array<string> or a set of array<bigint>");
			}
		}
		return ObjectInspectorFactory.getStandardListObjectInspector(
				PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(elemCategory));
	}

}

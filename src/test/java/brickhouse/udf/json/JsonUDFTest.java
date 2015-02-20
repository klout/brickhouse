package brickhouse.udf.json;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.*;
import brickhouse.udf.json.*;

public class JsonUDFTest {

	@Test
	public void testConvertToCamelCase() {
		String underScore=  "this_text_has_underscores";
		
		String camel = FromJsonUDF.ToCamelCase(underScore);
		System.out.println(camel);
		
		Assert.assertEquals( "thisTextHasUnderscores", camel);
	}

	@Test
	public void testConvertFromCamelCase() {
		String camel=  "thisTextIsInCamelCase";
		
		String under = FromJsonUDF.FromCamelCase(camel);
		System.out.println(under);
		
		Assert.assertEquals( "this_text_is_in_camel_case", under);
	}

	@Test
	public void testJsonMapHandlesNulls() throws HiveException {
		JsonMapUDF cut = new JsonMapUDF();
		Object result = cut.evaluate( new DeferredObject[]{ new DeferredJavaObject(null) } );
		Assert.assertNull( result );
	}

}

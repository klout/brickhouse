package brickhouse.hbase;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import brickhouse.udf.json.InspectorHandle;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 *  Load data from HBase, and cache locally in memory, for faster access.
 *  
 *  Similar to using a distributed map, except the values are stored in HBase,
 *  and can be sharded across multiple nodes, so one can process elements which
 *  wouldn't fit into memory on a single node.
 *  
 *  This may be useful in situations where you would potentially have a cartesian 
 *   product ( bayesian topic assignment, similiarity clustering ), and would 
 *    want to avoid an extra join. 
 *    
 *   One can cache strings, or arbitrary Hive structures, by storing values as
 *     JSON strings, and using a template object similiar to the one used in 
 *     the from_json UDF. An example would be storing a map&lt;string,double&gt; as 
 *     a bag-of-words, or an array&lt;string&gt; to store a sketch-set
 *    
 *
 */
@Description(name="hbase_cached_get",
value = "_FUNC_(configMap,key,template) - Returns a cached object, given an HBase config, a key, and a template object used to interpret JSON" 
)
public class CachedGetUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(CachedGetUDF.class);
	private Cache<String,Object> cache;
	private Map<String,String> configMap;
	private StringObjectInspector strInspector;
	private InspectorHandle jsonInspectorHandle;
	
	

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		return getValue( strInspector.getPrimitiveJavaObject( arg0[1].get()));
	}
	
	private CacheLoader valueLoader = new CacheLoader<String,Object>() {

		@Override
		public Object load(String key) throws Exception {
			String jsonString = loadString( key);
			if( (++numLoaded % 1000) ==0 ) {
				LOG.info(" loaded " + numLoaded+ " records; Key = " + key + " json =" + jsonString);
			}
			
			
			if( jsonInspectorHandle != null) {
			    ObjectMapper jacksonParser = new ObjectMapper();
				JsonNode jsonNode = jacksonParser.readTree( jsonString);
				
				return jsonInspectorHandle.parseJson( jsonNode);
			} else {
				return jsonString;
			}
		}
			

		public String loadString(String key) throws Exception {
			Get keyGet = new Get(key.getBytes());
			HTable htable = HTableFactory.getHTable( configMap);
			Result res = htable.get( keyGet);
			KeyValue kv = res.getColumnLatest(configMap.get( HTableFactory.FAMILY_TAG).getBytes() ,configMap.get( HTableFactory.QUALIFIER_TAG).getBytes() );
			if(kv == null) {
				throw new NoSuchElementException("No value found for " + key);
			}
			byte[] bytes = kv.getValue();
			String jsonStr =  new String(bytes);
			
			return jsonStr;
		}
		
	};
	
	private int numLoaded = 0;
    private int numCalls = 0;
	private int numMisses = 0;
	private int numErrors = 0;
	
	public Object getValue(final String key) {
		try {
            ++numCalls;
			Object l = cache.get(key, new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    ++numMisses;
                    return valueLoader.load(key);
                }
            });
			if( ((numCalls - numMisses) % 1000) == 0 ) {
				LOG.info( "Retrieved " + (numCalls - numMisses) + " features  key = " + key + " Num misses =" + numMisses);
			}
			return l;
		} catch (UncheckedExecutionException e) {
			LOG.error("Error while parsing string " , e);
			if( (++numErrors % 1000) == 0 ) {
				LOG.info( "Num Errors = " + numErrors + ";  Missed " + numMisses + " features key = " + key + " Num hits = " + (numCalls - numMisses));
			}
			return null;
		} catch (Exception unexpected) {
			LOG.error("Error while parsing string " , unexpected);
			if( (++numErrors % 1000) == 0 ) {
				LOG.info( "Num Errors = " + numErrors + "; Missed " + numMisses + " features key = " + key + " Num hits = " + (numCalls - numMisses));
			}
			return null;
		}
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "hbase_cache_array(" + arg0[0] + " , " + arg0[1] + ")";
	}

   /**
    *   User should pass in a constant Map of HBase parameters, 
    *     the String key to look up,
    *     
	*/
	@Override
	public ObjectInspector initialize(ObjectInspector[] parameters)
			throws UDFArgumentException {
		
		this.configMap = HTableFactory.getConfigFromConstMapInspector(parameters[0]);
		this.strInspector = (StringObjectInspector) parameters[1];
		
		this.cache = CacheBuilder.newBuilder().build(valueLoader);
		
		/**
		 *  If a third parameter is passed in, then 
		 */
		if( parameters.length > 2) {
			jsonInspectorHandle = InspectorHandle.InspectorHandleFactory.GenerateInspectorHandle( parameters[2]);
			
			return jsonInspectorHandle.getReturnType();
			
		} else {
			return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		}
	}
	

}

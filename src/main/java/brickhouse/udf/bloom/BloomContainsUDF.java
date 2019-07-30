package brickhouse.udf.bloom;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Returns true if the bloom (probably) contains the string
 */
@Description(
        name = "bloom_contains",
        value = " Returns true if the referenced bloom filter contains the key.. \n " +
                "_FUNC_(string key, string bloomfilter) "
)
public class BloomContainsUDF extends GenericUDF {

    private static Integer counter = 0;
    private static final Logger LOG = LoggerFactory.getLogger(BloomContainsUDF.class);
    private static BloomFactory factory = new BloomFactory();

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        // validation for the number of arguments
        if (OIs.length != 2) {
            throw new UDFArgumentException(getClass().getName() + " requires 2 args: key, bloom");
        }

        // output data type of the UDF
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Boolean evaluate(DeferredObject[] args) throws HiveException {
        String key = args[0].get().toString();
        String bloomFilter = args[1].get().toString();
        counter ++;
        long startTime = System.nanoTime();
        Filter bloom = factory.ReadBloomFromStringCached(bloomFilter);
        long endTime   = System.nanoTime();
        if (counter % 1000 == 0) {
            LOG.debug(String.format("Serialization time: %d",endTime - startTime));
        }
        if (bloom != null) {
            Boolean res = bloom.membershipTest(new Key(key.getBytes()));
            long searchEndTime = System.nanoTime();
            if (counter % 1000 == 0) {
                LOG.debug(String.format("Membership time: %d",searchEndTime - endTime));
            }
            return res;

        } else {
            throw new HiveException("Unable to find bloom " + bloomFilter);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "bloom_contains<key, bloom>";
    }
}

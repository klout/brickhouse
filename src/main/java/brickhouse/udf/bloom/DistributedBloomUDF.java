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
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * UDF to acccess a bloom stored from a file stored in distributed cache
 * <p/>
 * Assumes the file is a tab-separated file of name-value pairs,
 * which has been placed in distributed cache using the "add file" command
 * <p/>
 * Example
 * <p/>
 * INSERT OVERWRITE LOCAL DIRECTORY mybloom select bloom(key) from my_map_table where premise=true;
 * ADD FILE mybloom;
 * <p/>
 * select *
 * from my_big_table
 * where bloom_contains( key, distributed_bloom('mybloom') ) == true;
 */
@Description(
        name = "distribute_bloom",
        value = " Loads a bloomfilter from a file in distributed cache, and makes available as a named bloom. \n " +
                "_FUNC_(string filename) \n" +
                "_FUNC_(string filename, boolean returnEncoded) "
)
@UDFType(deterministic = false)
public class DistributedBloomUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(DistributedBloomUDF.class);
    private StringObjectInspector fnameInspector;
    private BooleanObjectInspector boolInspector;


    /**
     * BloomFilters need to be single files right now, containing only one
     * bloom filter
     *
     * @param mapFilename
     * @return
     * @throws IOException
     */
    static Filter loadBloom(String mapFilename) throws IOException {
        File mapFile = new File(mapFilename);
        if (!mapFile.exists()) {
            throw new FileNotFoundException(mapFilename + " not found.");
        }

        if (mapFile.isDirectory()) {
            String[] subFiles = mapFile.list();
            for (String subFile : subFiles) {
                if (subFile.endsWith("crc")) {
                    LOG.info(" Ignoring CRC file " + mapFilename);
                    continue;
                }
                else if(subFile.contains("hive-staging")){
                    LOG.info(" Ignoring hive staging file " + mapFilename);
                    continue;
                }

                else {
                    FileInputStream inStream = new FileInputStream(mapFilename + "/" + subFile);
                    return BloomFactory.ReadBloomFromStream(inStream);
                }
            }
            throw new FileNotFoundException(mapFilename + " not found.");
        } else {
            FileInputStream inStream = new FileInputStream(mapFilename);
            return BloomFactory.ReadBloomFromStream(inStream);
        }
    }


    /**
     * Load a BloomFilter to the local in memory cache ...
     *
     * @param mapFilename
     * @param returnEncoded
     * @return
     */
    public String evaluate(String mapFilename, Boolean returnEncoded) throws HiveException {
        try {
            LOG.info(" Map file " + mapFilename);
            Filter bloom = BloomFactory.GetNamedBloomFilter(mapFilename);
            if (bloom == null) {
                bloom = this.loadBloom(mapFilename);
                BloomFactory.PutNamedBloomFilter(mapFilename, bloom);
            }
            if (returnEncoded) {
                return BloomFactory.WriteBloomToString(bloom);
            } else {
                return mapFilename;
            }
        } catch (IOException ioExc) {
            throw new RuntimeException(ioExc);
        }
    }

    public String evaluate(String mapFilename) throws HiveException {
        return evaluate(mapFilename, false);
    }


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        String fname = fnameInspector.getPrimitiveJavaObject(arg0[0].get());
        boolean retEnc = false;
        if (this.boolInspector != null) {
            retEnc = boolInspector.get(arg0[1].get());
        }
        return evaluate(fname, retEnc);
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "distributed_bloom( " + arg0[0] + " ) ";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 1 && arg0.length != 2) {
            throw new UDFArgumentException("distributed_bloom takes a string and a boolean argument");
        }
        if (arg0[0].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("distributed_bloom takes a string and a boolean argument");
        } else {
            PrimitiveObjectInspector primInsp = (PrimitiveObjectInspector) arg0[0];
            if (primInsp.getPrimitiveCategory() != PrimitiveCategory.STRING) {
                throw new UDFArgumentException("distributed_bloom takes a string and a boolean argument");
            } else {
                this.fnameInspector = (StringObjectInspector) primInsp;
            }
        }
        if (arg0.length > 1) {
            if (arg0[1].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentException("distributed_bloom takes a string and a boolean argument");
            } else {
                PrimitiveObjectInspector primInsp = (PrimitiveObjectInspector) arg0[1];
                if (primInsp.getPrimitiveCategory() != PrimitiveCategory.BOOLEAN) {
                    throw new UDFArgumentException("distributed_bloom takes a string and a boolean argument");
                } else {
                    this.boolInspector = (BooleanObjectInspector) primInsp;
                }
            }
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }


}

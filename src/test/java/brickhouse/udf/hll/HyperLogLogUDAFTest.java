package brickhouse.udf.hll;

import io.airlift.stats.cardinality.HyperLogLog;
import io.airlift.slice.Slices;
import junit.framework.Assert;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

@Ignore("not ready yet")
public class HyperLogLogUDAFTest {
    private static final Logger LOG = Logger.getLogger(HyperLogLogUDAFTest.class);

    private static String TEST_HEADER = "\n************************************************************************\nRunning Test: ";

    @Test
    public void testSingleRowNullReturnsNull() throws HiveException {
        LOG.info(TEST_HEADER + "testSingleRowNullReturnsNull");

        HyperLogLogUDAF udaf = new HyperLogLogUDAF();
        ObjectInspector[] inputOiList = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputOiList, false, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        Mode m = Mode.COMPLETE;
        ObjectInspector outputOi = udafEvaluator.init(m, inputOiList);

        Object[] parameters = new Object[]{null, 12};
        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.iterate(agg, parameters);
        Object result = udafEvaluator.terminate(agg);

        LOG.info("result = " + result);

        Assert.assertNull(result);
    }

    @Test
    public void testMultipleRowNullReturnsNull() throws HiveException {
        LOG.info(TEST_HEADER + "testMultipleRowNullReturnsNull");

        HyperLogLogUDAF udaf = new HyperLogLogUDAF();
        ObjectInspector[] inputOiList1 = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        };

        ObjectInspector[] inputOiList2 = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        };

        GenericUDAFParameterInfo paramInfo1 = new SimpleGenericUDAFParameterInfo(inputOiList1, false, false, false);
        GenericUDAFEvaluator udafEvaluator1 = udaf.getEvaluator(paramInfo1);

        GenericUDAFParameterInfo paramInfo2 = new SimpleGenericUDAFParameterInfo(inputOiList2, false, false, false);
        GenericUDAFEvaluator udafEvaluator2 = udaf.getEvaluator(paramInfo2);

        Mode m1 = Mode.PARTIAL1;
        ObjectInspector partialOutputOi1 = udafEvaluator1.init(m1, inputOiList1);
        AggregationBuffer agg1 = udafEvaluator1.getNewAggregationBuffer();
        udafEvaluator1.reset(agg1);
        udafEvaluator1.iterate(agg1, new Object[]{null, 12});
        Object res1 = udafEvaluator1.terminate(agg1);

        Mode m2 = Mode.PARTIAL1;
        ObjectInspector partialOutputOi2 = udafEvaluator2.init(m2, inputOiList2);
        AggregationBuffer agg2 = udafEvaluator2.getNewAggregationBuffer();
        udafEvaluator2.reset(agg2);
        udafEvaluator2.iterate(agg2, new Object[]{null, 12});
        Object res2 = udafEvaluator2.terminate(agg2);

        ObjectInspector finalOutputOi = udafEvaluator2.init(Mode.FINAL, new ObjectInspector[]{partialOutputOi1});

        AggregationBuffer agg3 = udafEvaluator2.getNewAggregationBuffer();
        udafEvaluator2.merge(agg3, agg1);
        udafEvaluator2.merge(agg3, agg2);

        Object result = udafEvaluator2.terminate(agg3);

        LOG.info("result = " + result);

        Assert.assertNull(result);
    }

    @Test
    public void testSingleRowNonNullReturnsNonNull() throws HiveException {
        LOG.info(TEST_HEADER + "testSingleRowNonNullReturnsNonNull");

        HyperLogLogUDAF udaf = new HyperLogLogUDAF();
        ObjectInspector[] inputOiList = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputOiList, false, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        Mode m = Mode.COMPLETE;
        ObjectInspector finalOutputOi = udafEvaluator.init(m, inputOiList);

        Object[] parameters = new Object[]{"foo", 12};
        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.iterate(agg, parameters);
        Object result = udafEvaluator.terminate(agg);

        LOG.info("result = " + result);

        Assert.assertNotNull(result);
    }

    private void testCardinalityEstimateWithinBounds(Integer precision, Long uniqueCount) throws HiveException, IOException {
        LOG.info("testCardinalityEstimateWithinBounds - precision = " + precision + " - uniqueCount = " + uniqueCount);

        HyperLogLogUDAF udaf = new HyperLogLogUDAF();
        ObjectInspector[] inputOiList = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputOiList, false, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        Mode m = Mode.COMPLETE;
        ObjectInspector finalOutputOi = udafEvaluator.init(m, inputOiList);

        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);

        String uuid;
        HashMap<String, Integer> h = new HashMap<String, Integer>();
        for (int i = 0; i < uniqueCount; i++) {
            uuid = UUID.randomUUID().toString();
            h.put(uuid, 1);
            udafEvaluator.iterate(agg, new Object[]{uuid, precision});
        }

        Object result = udafEvaluator.terminate(agg);
        Assert.assertNotNull(result);

        byte[] b = ((JavaBinaryObjectInspector) finalOutputOi).getPrimitiveJavaObject(result);
        HyperLogLog hll = HyperLogLog.newInstance(Slices.wrappedBuffer(b));
        Long cardEst = hll.cardinality();

        LOG.info("cardEst = " + cardEst);

        int actualUniques = h.keySet().size();
        LOG.info("actualUniques = " + actualUniques);

        Long absDiff = Math.abs(cardEst - actualUniques);
        LOG.info("absDiff = " + absDiff);

        Double relDiff = absDiff.doubleValue() / uniqueCount.doubleValue();
        LOG.info("relDiff = " + relDiff);

        Double maxError = 1.04d / Math.sqrt(Math.pow(2, precision));
        LOG.info("maxError = " + maxError);

        Assert.assertTrue(relDiff < maxError);
    }

    @Test
    public void testCardinalityEstimateWithinBounds12() throws HiveException, IOException {
        LOG.info(TEST_HEADER + "testCardinalityEstimateWithinBounds12");

        testCardinalityEstimateWithinBounds(12, 1000000L);
    }

    @Test
    public void testCardinalityEstimateWithinBounds16() throws HiveException, IOException {
        LOG.info(TEST_HEADER + "testCardinalityEstimateWithinBounds16");

        testCardinalityEstimateWithinBounds(16, 1000000L);
    }

}

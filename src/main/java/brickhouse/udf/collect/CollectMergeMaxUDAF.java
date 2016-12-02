package brickhouse.udf.collect;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.*;

public class CollectMergeMaxUDAF extends AbstractCollectMergeUDAF {

    @Override
    public Map<PrimitiveCategory, Class<? extends CollectMergeUDAFEvaluator>> evaluators() {
        return new HashMap<PrimitiveCategory, Class<? extends CollectMergeUDAFEvaluator>>() {{
            put(BOOLEAN, BooleanEvaluator.class);
            put(BYTE, ByteEvaluator.class);
            put(SHORT, ShortEvaluator.class);
            put(INT, IntEvaluator.class);
            put(LONG, LongEvaluator.class);
            put(FLOAT, FloatEvaluator.class);
            put(DOUBLE, DoubleEvaluator.class);
            put(DATE, ComparableEvaluator.class);
            put(DECIMAL, ComparableEvaluator.class);
            put(STRING, ComparableEvaluator.class);
            put(TIMESTAMP, ComparableEvaluator.class);
            put(VARCHAR, ComparableEvaluator.class);
        }};
    }

    public static class BooleanEvaluator extends CollectMergeUDAFEvaluator {
        public BooleanEvaluator() {
        }

        @Override
        public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
            return new BooleanMergeMaxAggBuffer();
        }
    }

    public static class BooleanMergeMaxAggBuffer extends HashMapMergeAggBuffer<Boolean> {
        public BooleanMergeMaxAggBuffer() {
        }

        @Override
        public Boolean mergeValues(Boolean left, Boolean right) {
            return left ? left : right;
        }
    }


    public static class ByteEvaluator extends CollectMergeUDAFEvaluator {
        public ByteEvaluator() {
        }

        @Override
        public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
            return new ByteMergeMaxAggBuffer();
        }
    }

    public static class ByteMergeMaxAggBuffer extends HashMapMergeAggBuffer<Byte> {
        public ByteMergeMaxAggBuffer() {
        }

        @Override
        public Byte mergeValues(Byte left, Byte right) {
            return left >= right ? left : right;
        }
    }


    public static class ShortEvaluator extends CollectMergeUDAFEvaluator {
        public ShortEvaluator() {
        }

        @Override
        public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
            return new ShortMergeMaxAggBuffer();
        }
    }

    public static class ShortMergeMaxAggBuffer extends HashMapMergeAggBuffer<Short> {
        public ShortMergeMaxAggBuffer() {
        }

        @Override
        public Short mergeValues(Short left, Short right) {
            return left >= right ? left : right;
        }
    }


    public static class IntEvaluator extends CollectMergeUDAFEvaluator {
        public IntEvaluator() {
        }

        @Override
        public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
            return new IntMergeMaxAggBuffer();
        }
    }

    public static class IntMergeMaxAggBuffer extends HashMapMergeAggBuffer<Integer> {
        public IntMergeMaxAggBuffer() {
        }

        @Override
        public Integer mergeValues(Integer left, Integer right) {
            return left >= right ? left : right;
        }
    }


    public static class LongEvaluator extends CollectMergeUDAFEvaluator {
        public LongEvaluator() {
        }

        @Override
        public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
            return new LongMergeMaxAggBuffer();
        }
    }

    public static class LongMergeMaxAggBuffer extends HashMapMergeAggBuffer<Long> {
        public LongMergeMaxAggBuffer() {
        }

        @Override
        public Long mergeValues(Long left, Long right) {
            return left >= right ? left : right;
        }
    }


    public static class FloatEvaluator extends CollectMergeUDAFEvaluator {
        public FloatEvaluator() {
        }

        @Override
        public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
            return new FloatMergeMaxAggBuffer();
        }
    }

    public static class FloatMergeMaxAggBuffer extends HashMapMergeAggBuffer<Float> {
        public FloatMergeMaxAggBuffer() {
        }

        @Override
        public Float mergeValues(Float left, Float right) {
            return left >= right ? left : right;
        }
    }


    public static class DoubleEvaluator extends CollectMergeUDAFEvaluator {
        public DoubleEvaluator() {
        }

        @Override
        public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
            return new DoubleMergeMaxAggBuffer();
        }
    }

    public static class DoubleMergeMaxAggBuffer extends HashMapMergeAggBuffer<Double> {
        public DoubleMergeMaxAggBuffer() {
        }

        @Override
        public Double mergeValues(Double left, Double right) {
            return left >= right ? left : right;
        }
    }


    public static class ComparableEvaluator<T extends Comparable<? super T>> extends CollectMergeUDAFEvaluator {
        public ComparableEvaluator() {
        }

        @Override
        public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
            return new ComparableMergeMaxAggBuffer<T>();
        }
    }

    public static class ComparableMergeMaxAggBuffer<T extends Comparable<? super T>> extends HashMapMergeAggBuffer<T> {
        public ComparableMergeMaxAggBuffer() {
        }

        @Override
        public T mergeValues(T left, T right) {
            return left.compareTo(right) >= 0 ? left : right;
        }
    }
}

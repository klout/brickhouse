
CREATE TEMPORARY FUNCTION append_array AS 'brickhouse.udf.collect.AppendArrayUDF';
CREATE TEMPORARY FUNCTION array_index AS 'brickhouse.udf.collect.ArrayIndexUDF';
CREATE TEMPORARY FUNCTION array_union AS 'brickhouse.udf.collect.ArrayUnionUDF';
CREATE TEMPORARY FUNCTION first_index AS 'brickhouse.udf.collect.FirstIndexUDF';
CREATE TEMPORARY FUNCTION last_index AS 'brickhouse.udf.collect.LastIndexUDF';
CREATE TEMPORARY FUNCTION intersect_array AS 'brickhouse.udf.collect.ArrayIntersectUDF';

CREATE TEMPORARY FUNCTION array_flatten AS 'brickhouse.udf.collect.ArrayFlattenUDF';
CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';
CREATE TEMPORARY FUNCTION collect_distinct AS 'brickhouse.udf.collect.CollectDistinctUDAF';

CREATE TEMPORARY FUNCTION collect_max AS 'brickhouse.udf.collect.CollectMaxUDAF';
CREATE TEMPORARY FUNCTION collect_merge_max AS 'brickhouse.udf.collect.CollectMergeMaxUDAF';
CREATE TEMPORARY FUNCTION cast_array AS 'brickhouse.udf.collect.CastArrayUDF';
CREATE TEMPORARY FUNCTION cast_map AS 'brickhouse.udf.collect.CastMapUDF';
CREATE TEMPORARY FUNCTION combine AS 'brickhouse.udf.collect.CombineUDF';
CREATE TEMPORARY FUNCTION combine_unique AS 'brickhouse.udf.collect.CombineUniqueUDAF';
CREATE TEMPORARY FUNCTION conditional_emit AS 'brickhouse.udf.collect.ConditionalEmit';
CREATE TEMPORARY FUNCTION join_array AS 'brickhouse.udf.collect.JoinArrayUDF';
CREATE TEMPORARY FUNCTION map_filter_keys AS 'brickhouse.udf.collect.MapFilterKeysUDF';
CREATE TEMPORARY FUNCTION map_index AS 'brickhouse.udf.collect.MapIndexUDF';
CREATE TEMPORARY FUNCTION map_key_values AS 'brickhouse.udf.collect.MapKeyValuesUDF';
CREATE TEMPORARY FUNCTION mhash3 AS 'brickhouse.udf.collect.MurmurHash3UDF';
CREATE TEMPORARY FUNCTION mhash3array AS 'brickhouse.udf.collect.MurmurHash3ArrayUDF';
CREATE TEMPORARY FUNCTION multiday_count AS 'brickhouse.udf.collect.MultiDayCounterUDAF';
CREATE TEMPORARY FUNCTION numeric_range AS 'brickhouse.udf.collect.NumericRange';
CREATE TEMPORARY FUNCTION set_difference AS 'brickhouse.udf.collect.SetDifferenceUDF';
CREATE TEMPORARY FUNCTION truncate_array AS 'brickhouse.udf.collect.TruncateArrayUDF';
CREATE TEMPORARY FUNCTION union_max AS 'brickhouse.udf.collect.UnionMaxUDAF';
CREATE TEMPORARY FUNCTION union_map AS 'brickhouse.udf.collect.UnionUDAF';
CREATE TEMPORARY FUNCTION sessionize AS 'brickhouse.udf.collect.SessionizeUDF';
CREATE TEMPORARY FUNCTION group_count AS 'brickhouse.udf.collect.GroupCountUDF';
CREATE TEMPORARY FUNCTION group_first AS 'brickhouse.udf.collect.FirstOfGroupUDAF';

CREATE TEMPORARY FUNCTION json_map AS 'brickhouse.udf.json.JsonMapUDF';
CREATE TEMPORARY FUNCTION json_split AS 'brickhouse.udf.json.JsonSplitUDF';
CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';
CREATE TEMPORARY FUNCTION to_camel_case AS 'brickhouse.udf.json.ConvertToCamelCaseUDF';
CREATE TEMPORARY FUNCTION from_camel_case AS 'brickhouse.udf.json.ConvertFromCamelCaseUDF';

CREATE TEMPORARY FUNCTION assert AS 'brickhouse.udf.sanity.AssertUDF';
CREATE TEMPORARY FUNCTION assert_equals AS 'brickhouse.udf.sanity.AssertEqualsUDF';
CREATE TEMPORARY FUNCTION assert_less_than AS 'brickhouse.udf.sanity.AssertLessThanUDF';
CREATE TEMPORARY FUNCTION throw_error AS 'brickhouse.udf.sanity.ThrowErrorUDF';
CREATE TEMPORARY FUNCTION write_to_graphite AS 'brickhouse.udf.sanity.WriteToGraphiteUDF';
CREATE TEMPORARY FUNCTION write_to_tsdb AS 'brickhouse.udf.sanity.WriteToTSDBUDF';

CREATE TEMPORARY FUNCTION combine_previous_sketch AS 'brickhouse.udf.sketch.CombinePreviousSketchUDF';
CREATE TEMPORARY FUNCTION combine_sketch AS 'brickhouse.udf.sketch.CombineSketchUDF';
CREATE TEMPORARY FUNCTION convert_to_sketch AS 'brickhouse.udf.sketch.ConvertToSketchUDF';
CREATE TEMPORARY FUNCTION estimated_reach AS 'brickhouse.udf.sketch.EstimatedReachUDF';
CREATE TEMPORARY FUNCTION md5 AS 'brickhouse.udf.sketch.Md5';
CREATE TEMPORARY FUNCTION hash_md5 AS 'brickhouse.udf.sketch.HashMD5UDF';
CREATE TEMPORARY FUNCTION set_similarity AS 'brickhouse.udf.sketch.SetSimilarityUDF';
CREATE TEMPORARY FUNCTION sketch_set AS 'brickhouse.udf.sketch.SketchSetUDAF';
CREATE TEMPORARY FUNCTION sketch_hashes AS 'brickhouse.udf.sketch.SketchHashesUDF';
CREATE TEMPORARY FUNCTION union_sketch AS 'brickhouse.udf.sketch.UnionSketchSetUDAF';
CREATE TEMPORARY FUNCTION multiday_count AS 'brickhouse.udf.sketch.MultiDaySketcherUDAF';

CREATE TEMPORARY FUNCTION moving_avg AS 'brickhouse.udf.timeseries.MovingAvgUDF';
CREATE TEMPORARY FUNCTION sum_array AS 'brickhouse.udf.timeseries.SumArrayUDF';
CREATE TEMPORARY FUNCTION vector_add AS 'brickhouse.udf.timeseries.VectorAddUDF';
CREATE TEMPORARY FUNCTION vector_scalar_mult AS 'brickhouse.udf.timeseries.VectorMultUDF';
CREATE TEMPORARY FUNCTION vector_cross_product AS 'brickhouse.udf.timeseries.VectorCrossProductUDF';
CREATE TEMPORARY FUNCTION vector_dot_product AS 'brickhouse.udf.timeseries.VectorDotProductUDF';
CREATE TEMPORARY FUNCTION vector_magnitude AS 'brickhouse.udf.timeseries.VectorMagnitudeUDF';
CREATE TEMPORARY FUNCTION union_vector_sum AS 'brickhouse.udf.timeseries.VectorUnionSumUDAF';

CREATE TEMPORARY FUNCTION bloom AS 'brickhouse.udf.bloom.BloomUDAF';
CREATE TEMPORARY FUNCTION distributed_bloom AS 'brickhouse.udf.bloom.DistributedBloomUDF';
CREATE TEMPORARY FUNCTION bloom_contains AS 'brickhouse.udf.bloom.BloomContainsUDF';
CREATE TEMPORARY FUNCTION bloom_and AS 'brickhouse.udf.bloom.BloomAndUDF';
CREATE TEMPORARY FUNCTION bloom_or AS 'brickhouse.udf.bloom.BloomOrUDF';
CREATE TEMPORARY FUNCTION bloom_not AS 'brickhouse.udf.bloom.BloomNotUDF';

CREATE TEMPORARY FUNCTION add_days AS 'brickhouse.udf.date.AddDaysUDF';
CREATE TEMPORARY FUNCTION date_range AS 'brickhouse.udf.date.DateRangeUDTF';
CREATE TEMPORARY FUNCTION add_iso_period AS 'brickhouse.udf.date.AddISOPeriodUDF';

CREATE TEMPORARY FUNCTION hbase_balanced_key AS 'brickhouse.hbase.GenerateBalancedKeyUDF';
CREATE TEMPORARY FUNCTION hbase_batch_put AS 'brickhouse.hbase.BatchPutUDAF';
CREATE TEMPORARY FUNCTION hbase_batch_get AS 'brickhouse.hbase.BatchGetUDF';
CREATE TEMPORARY FUNCTION hbase_cached_get AS 'brickhouse.hbase.CachedGetUDF';
CREATE TEMPORARY FUNCTION hbase_get AS 'brickhouse.hbase.GetUDF';
CREATE TEMPORARY FUNCTION hbase_put AS 'brickhouse.hbase.PutUDF';
CREATE TEMPORARY FUNCTION salted_bigint_key AS 'brickhouse.hbase.SaltedBigIntUDF';

CREATE TEMPORARY FUNCTION salted_bigint AS 'brickhouse.hbase.SaltedBigIntUDF';


CREATE TEMPORARY FUNCTION hyperloglog AS 'brickhouse.udf.hll.HyperLogLogUDAF';
CREATE TEMPORARY FUNCTION union_hyperloglog AS 'brickhouse.udf.hll.UnionHyperLogLogUDAF';
CREATE TEMPORARY FUNCTION combine_hyperloglog AS 'brickhouse.udf.hll.CombineHyperLogLogUDF';
CREATE TEMPORARY FUNCTION hll_est_cardinality AS 'brickhouse.udf.hll.EstimateCardinalityUDF';




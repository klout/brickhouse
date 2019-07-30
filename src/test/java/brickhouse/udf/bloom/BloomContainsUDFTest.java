package brickhouse.udf.bloom;


import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BloomContainsUDFTest {

    @Test
    public void profileBloomContains() throws java.io.IOException, HiveException {
        int numElems = 1000000;
        double pct = 0.01;
        Filter bloom = BloomFactory.NewBloomInstance(numElems, pct);
        List<String> elemsToCheck = new ArrayList<>();
        for (int i = 0; i < numElems; ++i) {
            UUID uuid = UUID.randomUUID();

            Key key = new Key(uuid.toString().getBytes());
            bloom.add(key);

            Assert.assertTrue(bloom.membershipTest(key));
            if ((i % 10000) == 0) {
                System.out.println(" Added " + i + " elements.");
//                1 hit 1 miss
                elemsToCheck.add(uuid.toString());
                elemsToCheck.add(UUID.randomUUID().toString());
            }
        }

        String bfs = BloomFactory.WriteBloomToString(bloom);
        BloomContainsUDF udf = new BloomContainsUDF();
        Integer hitCounter = 0;
        Integer missCounter = 0;
        GenericUDF.DeferredObject[] args;
        for (String elem: elemsToCheck) {
            args = new GenericUDF.DeferredObject[]{
                    new GenericUDF.DeferredJavaObject(elem), new GenericUDF.DeferredJavaObject(bfs)};
            Boolean res = udf.evaluate(args);
            if (res) {
                hitCounter++;
            } else {
                missCounter++;
            }
        }
        System.out.println("Hits:" + hitCounter);
        System.out.println("Misses:" + missCounter);
        Assert.assertEquals(hitCounter, missCounter, elemsToCheck.size()/100);
    }
}

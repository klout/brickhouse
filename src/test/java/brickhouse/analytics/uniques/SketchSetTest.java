package brickhouse.analytics.uniques;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import brickhouse.udf.sketch.SetSimilarityUDF;
import brickhouse.udf.json.GetMainCategoryUDF;
import brickhouse.udf.json.GetDeviceUDF;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class SketchSetTest {

	@Test
	public void testSketchSet() {
		SketchSet ss = new SketchSet();
		ss.addHash(1);
		ss.addHash(2);
		ss.addHash(3);

		double card = ss.estimateReach();
		Assert.assertEquals( 3.0, card, 0.0);
	}

	@Test
	public void testDupSketchSet() {
		SketchSet ss = new SketchSet();
		ss.addHash(-11);
		ss.addHash(-11);
		ss.addHash(1);
		ss.addHash(2);
		ss.addHash(2);
		ss.addHash(3);
		ss.addHash(3);

		double card = ss.estimateReach();
		Assert.assertEquals( 4.0, card, 0.0);
	}

	@Test
	public void testDupSketchSetOver5000() {
		SketchSet ss = new SketchSet();
		int numHashes = (int) ( 5000 + Math.random()*1024*4);
		long minHash = Long.MAX_VALUE;
		for( int i=0; i<numHashes; ++i ) {
			double randHash = (Math.random()*((double)Long.MAX_VALUE)*2 ) - (double)Long.MAX_VALUE;
			if( randHash < minHash)
				minHash = (long)randHash;
			ss.addItem( "" + randHash);
		}
		double ratio = ss.estimateReach()/(double)numHashes;
		//System.out.println(" Estimate reach = " + ss.estimateReach() + " size = "  + numHashes  + " ratio = " + ratio);
		Assert.assertTrue( ratio > 0.9 && ratio < 1.1);


		ss.addHash( minHash -1 );
		numHashes++;
		ratio = ss.estimateReach()/(double)numHashes;
		//System.out.println(" Estimate reach = " + ss.estimateReach() + " size = "  + numHashes  + " ratio = " + ratio);
		Assert.assertTrue( ratio > 0.95 && ratio < 1.05);

		ss.addHash( minHash -1 );
		ratio = ss.estimateReach()/(double)numHashes;
		//System.out.println(" Estimate reach = " + ss.estimateReach() + " size = "  + numHashes  + " ratio = " + ratio);
		Assert.assertTrue( ratio > 0.95 && ratio < 1.05);

		ss.addHash( minHash -2 );
		numHashes++;
		ratio = ss.estimateReach()/(double)numHashes;
		//System.out.println(" Estimate reach = " + ss.estimateReach() + " size = "  + numHashes  + " ratio = " + ratio);
		Assert.assertTrue( ratio > 0.95 && ratio < 1.05);

		ss.addHash( minHash -2 );
		ratio = ss.estimateReach()/(double)numHashes;
		//System.out.println(" Estimate reach = " + ss.estimateReach() + " size = "  + numHashes  + " ratio = " + ratio);
		Assert.assertTrue( ratio > 0.95 && ratio < 1.05);


		long lastHash = ss.lastHash();
		ss.addHash( lastHash - 1);
		numHashes++;
		ratio = ss.estimateReach()/(double)numHashes;
		//System.out.println(" Estimate reach = " + ss.estimateReach() + " size = "  + numHashes  + " ratio = " + ratio);
		Assert.assertTrue( ratio > 0.95 && ratio < 1.05);

	}



	@Test
	public void testRandomHashes() {
		SketchSet ss = new SketchSet();
		int numHashes = 1024*1024;
		for( int i=0; i<numHashes; ++i ) {
			double randHash = Math.random()*((double)Long.MAX_VALUE);
			if(Math.random() < 0.5) {
				randHash = -1*randHash;
			}
			ss.addHash((long)randHash);
		}

		double card = ss.estimateReach();
		double tolerance = 0.05;


		int diff = (int) Math.abs( card - numHashes);
		double diffRatio = ((double)diff)/numHashes;
		//System.out.println(" Estimated cardinality is " +card + " ; Expected " + numHashes + " ; Difference was " + diff + " ; diff ratio is " + diffRatio);
		Assert.assertTrue( diffRatio <= tolerance);

		//System.out.println(" Estimated cardinality is " +card);
		Assert.assertEquals( numHashes, card, numHashes*tolerance);
	}


	///@Test
	public void testManyRandomHashes() {
		double maxDiff = 0;
		double totDiff = 0;
		int numRuns =512;
		long now = System.currentTimeMillis();
		for(int j=0; j<numRuns; ++j) {
			SketchSet ss = new SketchSet();
			int numHashes = (int) (Math.random()*1024*514);
			for( int i=0; i<numHashes; ++i ) {
				double randHash = Math.random()*((double)Long.MAX_VALUE);
				if(Math.random() < 0.5) {
					randHash = -1*randHash;
				}
				ss.addHash((long)randHash);
			}

			double card = ss.estimateReach();
			double tolerance = 0.05;

			int diff = (int) Math.abs( card - numHashes);
			double diffRatio = ((double)diff)/numHashes;
			//System.out.println(" J = " + j);
			//System.out.println(" Estimated cardinality is " +card + " ; Expected " + numHashes + " ; Difference was " + diff + " ; diff ratio is " + diffRatio);
			Assert.assertTrue( diffRatio <= tolerance);
			if(diffRatio > maxDiff)
				maxDiff = diffRatio;

			totDiff+= diffRatio;
			//System.out.println(" Estimated cardinality is " +card);
			Assert.assertEquals( numHashes, card, numHashes*tolerance);
		}
		long later = System.currentTimeMillis();
		int numSecs = (int) ((later -now)/1000.0);
		//System.out.println(" Max Diff Ratio = " + maxDiff);
		double avgDiff = totDiff/(double)numRuns;
		//System.out.println(" Avg Diff Ratio = " + avgDiff);

		//System.out.print( numRuns + " took " + numSecs);
	}


	@Test
	public void testHashStrings() {
		SketchSet ss = new SketchSet(5000);
		int numHashes = 512*1024;
		for(int i=0; i<numHashes; ++i) {
			UUID randomUUID = UUID.randomUUID();
			///System.out.println(" RandomUUID " + randomUUID.toString());
			ss.addItem( randomUUID.toString());
		}

		double card = ss.estimateReach();
		double tolerance = 0.05;

		int diff = (int) Math.abs( card - numHashes);
		double diffRatio = ((double)diff)/numHashes;
		//System.out.println(" Estimated cardinality is " +card + " ; Expected " + numHashes + " ; Difference was " + diff + " ; diff ratio is " + diffRatio);
		Assert.assertTrue( diffRatio <= tolerance);
	}

	@Test
	public void testDistinctSets() {

		SketchSet ss = new SketchSet(5000);
		int numHashes1 = (int) ((double)(1024*512)*Math.random());
		//System.out.println(" Number of hashes one = " + numHashes1);
		for(int i=0; i<numHashes1; ++i) {
			UUID randomUUID = UUID.randomUUID();
			///System.out.println(" RandomUUID " + randomUUID.toString());
			ss.addItem( randomUUID.toString());
		}

		double card = ss.estimateReach();
		//System.out.println(" Estimated Card 1 = " + card);


		SketchSet ss2 = new SketchSet(5000);
		int numHashes2 = (int) ((double)(1024*512)*Math.random());
		//System.out.println(" Number of hashes two = " + numHashes2);
		for(int i=0; i<numHashes2; ++i) {
			UUID randomUUID = UUID.randomUUID();
			///System.out.println(" RandomUUID " + randomUUID.toString());
			ss2.addItem( randomUUID.toString());
		}

		double card2 = ss2.estimateReach();
		//System.out.println(" Estimated Card 2 = " + card2);

		ss.combine( ss2);

		double newCard = ss.estimateReach();
		//System.out.println(" Sum of hashes  = " + ( card + card2));
		//System.out.println(" Estimated Combined Card = " + newCard);


		//System.out.println(" New Card = " + newCard);

	}

	@Test
	public void testOverlapSets() {

		SketchSet ss = new SketchSet(5000);
		int numHashes1 = (int) ((double)(1024*512)*Math.random());
		//System.out.println(" Number of hashes one = " + numHashes1);
		for(int i=0; i<numHashes1; ++i) {
			UUID randomUUID = UUID.randomUUID();
			///System.out.println(" RandomUUID " + randomUUID.toString());
			ss.addItem( randomUUID.toString());
		}

		double card = ss.estimateReach();
		//System.out.println(" Estimated Card 1 = " + card);


		SketchSet ss2 = new SketchSet(5000);
		int numHashes2 = (int) ((double)(1024*512)*Math.random());
		//System.out.println(" Number of hashes two = " + numHashes2);
		for(int i=0; i<numHashes2; ++i) {
			UUID randomUUID = UUID.randomUUID();
			///System.out.println(" RandomUUID " + randomUUID.toString());
			ss2.addItem( randomUUID.toString());
		}

		double card2 = ss2.estimateReach();
		//System.out.println(" Estimated Card 2 = " + card2);

		SketchSet ss3 = new SketchSet(5000);
		int numHashes3 = (int) ((double)(1024*512)*Math.random());
		//System.out.println(" Number of hashes three = " + numHashes3);
		for(int i=0; i<numHashes3; ++i) {
			UUID randomUUID = UUID.randomUUID();
			///System.out.println(" RandomUUID " + randomUUID.toString());
			ss3.addItem( randomUUID.toString());
		}

		double card3 = ss3.estimateReach();
		//System.out.println(" Estimated Card 3= " + card3);

		ss.combine( ss2);


		ss.combine( ss2);

		double cardCombine1 = ss.estimateReach();
		//System.out.println(" Combine 1 = " + cardCombine1);
		//System.out.println(" 1 +2 " + ( numHashes1 + numHashes2 ));

		ss3.combine( ss2);
		double cardCombine3 = ss3.estimateReach();
		//System.out.println(" Combine 3 = " + cardCombine3);
		//System.out.println(" 2  + 3 " + ( numHashes3 + numHashes2 ));

		SketchSet overLap = new SketchSet();
		overLap.combine( ss);
		overLap.combine( ss3 );

		double cardOverlap = overLap.estimateReach();
		//System.out.println(" Overlap = " + cardOverlap);
		//System.out.println(" All hashes = " + ( numHashes1 + numHashes2 + numHashes3));

	}


	@Test
	public void testObama() throws IOException {
		SketchSet ss = new SketchSet();
		SketchSet ss2 = new SketchSet();

		HashFunction md5 = Hashing.md5();

		System.out.println(" Directory is " + System.getProperty("user.dir"));
		FileInputStream fs = new FileInputStream("src/test/resources/obama.txt");
		int cnt =0;
		BufferedReader reader = new BufferedReader( new InputStreamReader(fs ));
		String line;
		while( (line = reader.readLine() ) != null) {
			ss.addItem( line);
			ss2.addHashItem(  md5.hashString( line).asLong(), line );
			cnt++;
		}

		//System.out.println(" Estimated Reach = " + ss.estimateReach() + " count = " + cnt);
		double diff = cnt - ss.estimateReach();
		double pctDiff = Math.abs( diff/(double)cnt);
		//System.out.println( " Difference is " + pctDiff);

		Assert.assertTrue( pctDiff < 0.03);

		//System.out.println(" Estimated Reach = " + ss2.estimateReach() + " count = " + cnt);
		 diff = cnt - ss2.estimateReach();
		 pctDiff = Math.abs( diff/(double)cnt);
		//System.out.println( " Difference is " + pctDiff);

		Assert.assertTrue( pctDiff < 0.03);

		SortedMap<Long,String> hashItemMap = ss.getHashItemMap();
		//System.out.println( " First Key is " +  hashItemMap.firstKey() );
		//System.out.println( " Last Key is " +  hashItemMap.lastKey() );
	}


	@Test
	public void testGetMinHashes() {
		SketchSet ss = new SketchSet();

		int numHashes =  5100 + (int)(Math.random()*15000);
		for(int i=0; i<numHashes; ++i) {
			UUID randomUUID = UUID.randomUUID();
			///System.out.println(" RandomUUID " + randomUUID.toString());
			ss.addItem( randomUUID.toString());
		}

		List<Long> md5Hashes = ss.getMinHashes();
		long last = Long.MIN_VALUE;
		for( long md5 : md5Hashes) {
			Assert.assertTrue( md5 > last);
			last = md5;
		}
		double estReach = SketchSet.EstimatedReach( last, ss.getMaxItems());
		double ratio = estReach/(double)numHashes;
		//System.out.println( " Estimated Reach = " + estReach + " num Hashes  = " + numHashes + " ; Ratio = " + ratio);
		Assert.assertTrue( ratio < 1.05 && ratio > 0.95);

	}

	@Test
	public void testSetSimilarity() {
		int numHashes = 200000;
		SketchSet a = new SketchSet();
		SketchSet b = new SketchSet();
		SketchSet c = new SketchSet();

		for(int i=0; i<numHashes; ++i) {
			UUID randomUUID = UUID.randomUUID();
			///System.out.println(" RandomUUID " + randomUUID.toString());
			a.addItem( randomUUID.toString());
			randomUUID = UUID.randomUUID();
			b.addItem( randomUUID.toString());
			randomUUID = UUID.randomUUID();
			c.addItem( randomUUID.toString());
		}
		SetSimilarityUDF simUDF = new SetSimilarityUDF();

		double same = simUDF.evaluate(a.getMinHashItems(), a.getMinHashItems());
		//System.out.println( "Similarity with self = " + same);
		Assert.assertEquals( 1.0, same, 0);

		double diff = simUDF.evaluate(a.getMinHashItems(), b.getMinHashItems());
		//System.out.println( "Similarity with different  = " + diff);
		Assert.assertEquals( 0, diff , 0.03); /// Might not be quite zero

		a.combine(c);
		b.combine(c);

		double mixed = simUDF.evaluate( a.getMinHashItems(), b.getMinHashItems());
		//System.out.println("Similarity with mixed = " +mixed);
		//// Should be about a third
		Assert.assertEquals( 0.333333333, mixed, 0.03);

	}

	@Test
	public void testGetMainCategoryUDF() {
		GetMainCategoryUDF test = new GetMainCategoryUDF();
		String result = test.evaluate("[{\"id\":\"896\",\"parents\":[],\"name\":\"Hardware\"},{\"id\":\"900\",\"parents\":[\"896\"],\"name\":\"Armazenamento\"},{\"id\":\"904\",\"parents\":[\"900\"],\"name\":\"SSD\"},{\"id\":\"1027\",\"parents\":[],\"name\":\"Promoc3a7c3b5es\"},{\"id\":\"1409\",\"parents\":[\"1027\"],\"name\":\"BoxingWeek\"},{\"id\":\"1424\",\"parents\":[\"1027\"],\"name\":\"Festival de Hardware\"},{\"id\":\"1425\",\"parents\":[\"1027\"],\"name\":\"Festival de informc3a1tica\"}]");
		System.out.println(result);
		Assert.assertTrue(result.equals("{\"id\":\"896\",\"parents\":[],\"name\":\"Hardware\"}"));
	}

	@Test
	public void testGetMainCategoryUDF_2() {
		GetMainCategoryUDF test = new GetMainCategoryUDF();
		String result = test.evaluate("[{\"id\":\"Livros\",\"parents\":\"null\",\"name\":\"Livros\"},{\"id\":\"900\",\"parents\":[\"896\"],\"name\":\"Armazenamento\"},{\"id\":\"904\",\"parents\":[\"900\"],\"name\":\"SSD\"},{\"id\":\"1027\",\"parents\":[],\"name\":\"Promoc3a7c3b5es\"},{\"id\":\"1409\",\"parents\":[\"1027\"],\"name\":\"BoxingWeek\"},{\"id\":\"1424\",\"parents\":[\"1027\"],\"name\":\"Festival de Hardware\"},{\"id\":\"1425\",\"parents\":[\"1027\"],\"name\":\"Festival de informc3a1tica\"}]");
		System.out.println(result);
		Assert.assertTrue(result.equals("{\"id\":\"Livros\",\"parents\":\"null\",\"name\":\"Livros\"}"));
	}

	@Test
	public void testGetDeviceUDF() {
		GetDeviceUDF test = new GetDeviceUDF();
		String result = test.evaluate("Windows Phone 8.1", null);
		System.out.println(result);
		Assert.assertTrue(result.equals("mobile"));
	}

	@Test
	public void testGetDeviceUDF_2() {
		GetDeviceUDF test = new GetDeviceUDF();
		String result = test.evaluate("Windows 8", null);
		System.out.println(result);
		Assert.assertTrue(result.equals("web"));
	}

	@Test
	public void testGetDeviceUDF_3() {
		GetDeviceUDF test = new GetDeviceUDF();
		String result = test.evaluate("Windows 8", "mobile-app");
		System.out.println(result);
		Assert.assertTrue(result.equals("api.mobile-app"));
	}

	@Test
	public void testGetDeviceUDF_4() {
		GetDeviceUDF test = new GetDeviceUDF();
		String result = test.evaluate("Windows 8", "desktop");
		System.out.println(result);
		Assert.assertTrue(result.equals("web"));
	}

	@Test
	public void testGetDeviceUDF_5() {
		GetDeviceUDF test = new GetDeviceUDF();
		String result = test.evaluate("Windows 8", "mobile");
		System.out.println(result);
		Assert.assertTrue(result.equals("mobile"));
	}
}

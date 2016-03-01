Welcome to the Brickhouse
=========================

   Brickhouse is a collection of UDF's for Hive to improve developer 
   productivity, and the scalability and robustness of Hive queries.
   

  Brickhouse covers a wide range of functionality, grouped in the 
     following packages.

 * _collect_ - An implementaion of "collect"  and various utilities
     for dealing with maps and arrays.
   
 * _json_ - Translate between Hive structures and JSON strings

 * _sketch_ - An implementation of KMV sketch sets, for reach 
     estimation of large datasets.

 * _bloom_ - UDF wrappers around the Hadoop BloomFilter implementation.

 * _sanity_ - Tools for implementing sanity checks and managing Hive
	  in a production environment.
   
 * _hbase_ - Experimental UDFs for an alternative way to integrate
	  Hive with HBase.

The ETL4 specific UDFs can be found in the _json_ package:
 * GetMainCategoryUDF
 * NullNormalizerUDF
 * JsonToArrayUDF
 * ClassifyDeviceUDF

 After updating an UDF, use "mvn package" to compile the brickhouse-0.7.0-SNAPSHOT.jar and copy it to the folder hive/s3_files at ETL4 project.
     
Requirements:
--------------
  Brickhouse require Hive 0.9.0 or later;
  Maven 2.0 and a Java JDK is required to build.

Getting Started
---------------
 1. Clone ( or fork ) the repo from  https://github.com/klout/brickhouse 
 2. Run "mvn package" from the command line.
 3. Add the jar "target/brickhouse-\<version number\>.jar" to your HIVE_AUX_JARS_FILE_PATH,
    or add it to the distributed cache from the Hive CLI 
    with the "add jar" command
 4. Source the UDF declarations defined in src/main/resource/brickhouse.hql

See the wiki on Github at https://github.com/klout/brickhouse/wiki for more 
  information.

Also, see discussions on the Brickhouse Confessions blog on Wordpress 
 
 http://brickhouseconfessions.wordpress.com/




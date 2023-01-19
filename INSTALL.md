

## Install pySpark on a big memory node
To install, first install conda and create a new environment. In the new environment:
```
pip install pyspark # this installs the latest version (pyspark 3.1.2)
#conda install pyspark # this installs an older version
```
Then install jupyter if you want to use the notebook, and add the following to your .bashrc to set $SPARK_HOME:

```
export PYSPARK_PYTHON=python3 
export SPARK_HOME=~/.conda/envs/pyspark/lib/python3.10/site-packages/pyspark
```
To use AWS S3:
```
wget https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk-bundle/1.12.357/aws-java-sdk-bundle-1.12.357.jar -O {SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.357.jar
wget https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -O {SPARK_HOME}/jars/hadoop-aws-3.3.2.jar
```
In a jupyter notebook, use the following to set up access to S3:
```
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyspark

aws_access_key_id = 'XXXX'
aws_secret_access_key = 'XXX'

spark = (SparkSession 
            .builder 
            .appName("1kgenome") 
            .master('local[72]')
            .config('spark.executor.heartbeatInterval', '1000s')
            .config('spark.network.timeout', '10000s')
            .config('spark.executor.memory', '4000G')
            .config('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
            .config('spark.driver.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true')
            .config('spark.kryoserializer.buffer.max', '1G') # higher causes oom （GC limit）
            .config('spark.local.dir','/mnt/data/spark') # /tmp might be full
            .config('spark.sql.autoBroadcastJoinThreshold', '-1')
            .getOrCreate()
        )
sc = spark.sparkContext
sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key_id)
hadoopConf.set('fs.s3a.secret.key', aws_secret_access_key)

```

To use PMem, 
```
mm -c jupyter.yml jupyter-notebook
```

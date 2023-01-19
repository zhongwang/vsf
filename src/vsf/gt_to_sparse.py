# convert genotype file (in parquet) to sparse format
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
genomes = spark.read.parquet('1kgenome_dbSNP328.pq')

samples = genomes.columns[9:-1]

# split alleles

a1 = (genomes
      .select(
        'ID',
        *((F.split(c, '\|').getItem(0).astype('int').alias(c) for c in samples))
      )
      .fillna(0)
      .withColumn('gts', F.array(*samples))
      .select('ID', 'gts')
     )

a2 = (genomes
      .select(
        'ID',
        *((F.split(c, '\|').getItem(1).astype('int').alias(c) for c in samples))
      )
      .fillna(0)
      .withColumn('gts', F.array(*samples))
      .select('ID', 'gts')
     )

from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
# this takes 64.6 hrs + 50.3 hrs
a1 = IndexedRowMatrix(a1.rdd.map(lambda row: IndexedRow(*row))).toCoordinateMatrix()
a2 = IndexedRowMatrix(a2.rdd.map(lambda row: IndexedRow(*row))).toCoordinateMatrix()

￼
a1 =(spark
 .createDataFrame(a1.transpose().entries)
 .where(F.col('value')>0)
 .toDF('sample', 'rsID', 'code')
)
a2 =(spark
 .createDataFrame(a2.transpose().entries)
 .where(F.col('value')>0)
 .toDF('sample', 'rsID', 'code')
)

# create the genotype sparse matrix
# rows are samples
# columns are genotype (rsID:gt)
# values are doses (how many alleles)
gt = a1.union(a2)
gt = (gt
      .withColumn('index', F.concat_ws(':', *['rsID', 'code']))
      .withColumn('dose', F.lit(1.0))
      .select('sample', 'index', 'dose')
      .groupby('sample', 'index')
      .agg(F.sum('dose').alias('dose'))
     )

gt.write.mode('overwrite').parquet('1kgenome_gt_coo.pq')

# create indexer
from pyspark.ml.feature import StringIndexer
indexx = gt.select('index')
indexer = StringIndexer(inputCol='index', outputCol='i').fit(indexx)
indexx = indexer.transform(indexx)
gt = gt.join(indexx, on='index', how='left').select('index', 'sample', 'i', 'dose')
gt.write.mode('overwrite').parquet('1kgenome_gt_indexed_coo.pq')

from pyspark.mllib.linalg.distributed import CoordinateMatrix

gt = CoordinateMatrix(gt.rdd.map(lambda r: (r[0], r[1], r[2])))
gwas = CoordinateMatrix(ref1k.rdd.map(lambda r: (r[0], r[1], r[2])))

results = gt.toRowMatrix().multiply(gwas.toRowMatrix())

# save the score matrix
(spark
 .createDataFrame(results.entries)
 .toDF('sample', 'trait', 'score')
 .write
 .mode('overwrite')
 .parquet('s3a://zhong-active/PGS/1kgenome_gwas_scores_coo.pq')
)
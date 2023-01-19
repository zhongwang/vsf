
# update rsIDs in a file to the latest in dbSNP

dbsnp = 's3a://zhong-active/PGS/dbsnp328/dbsnp328_grc37_p13.pq'

dbsnp = (spark
       .read
       .parquet(dbsnp)
       .select(F.col('chr').alias('CHROM'), F.col('pos').alias('POS'), F.col('rsID').alias('ID'))
       .drop_duplicates(['CHROM', 'POS'])
      )    

all_vcf = (all_vcf
        .withColumnRenamed('ID', 'oldID')
        .join(dbsnp, on=['CHROM', 'POS'], how='left')
        )

all_vcf.repartition(200).write.mode('overwrite').parquet('1kgenome_dbSNP328.pq')  
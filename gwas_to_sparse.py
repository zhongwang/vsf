# only need to run this once
gwas = 's3a://zhong-active/PGS/shared-dna/notebooks/gwas_2022-11-04.csv.gz'
grc37_p13 = 's3a://zhong-active/PGS/dbsnp328/dbsnp328_grc37_p13.pq'
grc38_p13 = 's3a://zhong-active/PGS/dbsnp328/dbsnp328_grc38_p13.pq'
dbsnp1 = spark.read.parquet(grc37_p13).select('chr', 'pos', 'rsID')
dbsnp2 = spark.read.parquet(grc38_p13).select('chr', 'pos', 'rsID')
ref = (spark
       .read
       .csv(gwas, sep='\t', header=True)
       .withColumn('CHR_ID', F.when(F.isnull('CHR_ID'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 1)).otherwise(F.col('CHR_ID')))
       .withColumn('CHR_POS', F.when(F.isnull('CHR_POS'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 2)).otherwise(F.col('CHR_POS')))       
       .select(F.col('SNP_ID_CURRENT').astype('long').alias('ID'), 
               F.col('rallele').alias('alt'),
               F.col('OR or BETA').astype('float').alias('OR'),
               F.col('CHR_ID').alias('chr'),
               F.col('CHR_POS').astype('long').alias('pos'),
               F.col('DISEASE/TRAIT').alias('trait'),
               F.col('DATE ADDED TO CATALOG').astype('date').alias('date'),
               F.col('PUBMEDID').astype('long').alias('pubmedID')
       ).where(
         (F.col('OR')>0.0)
       )
)

# replace chromosome numbering
mapping = {
  'X': '23',
  'Y': '24',
  'M': '25'
}
mapping.update({str(i+1):str(i+1) for i in range(22)})
print(mapping)
apply_mapping_udf = F.udf(lambda x: mapping.get(x, x))
ref = ref.withColumn('chr', apply_mapping_udf('chr').astype('int'))

ref = (ref
       .join(dbsnp1, on=['chr', 'pos'], how='left')
       .withColumn('ID', F.when(F.isnull('ID'), F.col('rsID')).otherwise(F.col('ID')))
       .drop('rsID')
       .join(dbsnp2, on=['chr', 'pos'], how='left')
       .withColumn('rsID', F.when(F.isnull('ID'), F.col('rsID')).otherwise(F.col('ID')))
       .select('trait', 'rsID', 'alt', 'OR', 'pubmedID', 'date')
      )               
ref = ref.drop_duplicates(['trait', 'rsID', 'alt'])
(ref
 .write
 .mode('overwrite')
 .parquet('s3a://zhong-active/PGS/shared-dna/notebooks/gwas_2022-11-04_ORs.pq')
)

# only run this once
# assign reference code to grc37
ref = (
  ref
  .join(grc37_p13.select('rsID', F.col('allele').alias('alt'),'code'), on=['rsID', 'alt'], how='left')
  .fillna({'code':0})
)

code = spark.read.parquet('s3a://zhong-active/PGS/1kgenome_dbSNP328_code_mapping.pq')
ref = spark.read.parquet('s3a://zhong-active/PGS/shared-dna/notebooks/gwas_2022-11-04_ORs.pq')
# mapping ref codes to 1kgenome codes
ref1k = ref.join(code.select(F.col('ID').alias('rsID'), 'code', 'code1k'), on=['rsID', 'code'], how='left').fillna(0)

# create gwas ref sparse matrix
# rows are genotype(rsID:gt)
# columns are OR

ref1k = (ref1k
 .withColumn('trait', F.concat_ws('|', *['trait', 'pubmedID']))
 .withColumn('index', F.concat_ws(':', *['rsID', 'code1k']))
 .select('index', 'trait', 'OR')
) 

# create indexer for traits
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol='trait', outputCol='j').fit(ref1k)
ref1k = indexer.transform(ref1k).select('index', 'j', 'OR', 'trait')


# match genotypes in reference gwas using the indexed 1kgenome
# variants not in the 1kgenome are ignored

ref1k = (
    ref1k
    .join(gt.select('index', 'i'), on='index', how='left')
    .fillna({'i': -1})
    .where(F.col('i')>=0)
    .select('i', 'j', 'OR', 'trait')
)


# save results
ref1k.write.mode('overwrite').parquet('gwas_2022-11-04_ORs_1kgenome_indexed_coo.pq')

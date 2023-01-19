# only need to run this once
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from vsf.utils import *

spark = SparkSession.builder.getOrCreate()

def gwas_to_sparse(gwas, output):
  """_summary_

  Args:
      gwas (_type_): _description_
      output (_type_): _description_
  """

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

  # save results
  ref1k.write.mode('overwrite').parquet(output)

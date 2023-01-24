# only need to run this once
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import StringIndexer

from vsf.utils import *

spark = SparkSession.builder.getOrCreate()

def gwas_csv_to_parquet(gwas, output=''):
  """_convert gwas result file to parquet_

  Args:
      gwas (_string_): _description_
      output (_string_): _output file_, if empty, return the dataframe
  """

  ref = (spark
        .read
        .csv(gwas, sep='\t', header=True)
        .withColumn('CHR_ID', F.when(F.isnull('CHR_ID'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 1)).otherwise(F.col('CHR_ID')))
        .withColumn('CHR_POS', F.when(F.isnull('CHR_POS'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 2)).otherwise(F.col('CHR_POS')))       
        .select(F.col('SNP_ID_CURRENT').astype('long').alias('rsID'), 
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
  ref = ref.withColumn('chr', chr_mapping[ref['chr']])

  if output == '':
        return ref
  (ref
  .write
  .mode('overwrite')
  .parquet(output)
  )

def gwas_fill_rsID(dbsnp, ref, out_file='', overwrite=False):
  """_summary_

  Args:
      dbsnp (_type_): _description_
      ref (_type_): _description_
      out_file (str, optional): _description_. Defaults to ''.
      overwrite (bool, optional): _description_. Defaults to False.

  Returns:
      _type_: _description_
  """

  ref = (ref
        .join(dbsnp.select('chr', 'pos', F.col('rsID').alias('newID')), on=['chr', 'pos'], how='left')
  )
  if overwrite:
      ref = ref.drop('rsID').withColumnRenamed('newID', 'rsID')     
  else:
      ref = (ref
        .withColumn('rsID', F.when(F.isnull('rsID'), F.col('newID')).otherwise(F.col('rsID')))
        .drop('newID')
        )               
  ref = ref.drop_duplicates(['trait', 'rsID', 'alt'])
  if out_file == '':
      return ref
  ref.write.mode('overwrite').parquet(out_file)

def gwas_add_code(dbsnp, ref, codemapping=None, out_file=''):
  """_summary_

  Args:
      dbsnp (_type_): _description_
      ref (_type_): _description_
      codemapping (_type_, optional): _description_. Defaults to None.
      out_file (str, optional): _description_. Defaults to ''.

  Returns:
      _type_: _description_
  """

  ref = (
    ref
    .join(dbsnp.select('rsID', F.col('allele').alias('alt'),'code'), on=['rsID', 'alt'], how='left')
    .fillna({'code':0})
  )
  if codemapping:
    # mapping ref codes to 1kgenome codes
    ref = ref.join(codemapping.select(F.col('ID').alias('rsID'), 'code', 'code1k'), on=['rsID', 'code'], how='left').fillna(0)
    
  # add index
    ref = ref.withColumn('index', F.concat_ws('|', *['trait', 'pubmedID']))
    rindex = ref.select('index')
    indexer = StringIndexer(inputCol='index', outputCol='i').fit(rindex)
    ref = indexer.transform(ref).join(rindex, on='index', how='left')
  if out_file == '':
      return ref
  ref.write.mode('overwrite').parquet(out_file)      

def gwas_to_vsf(gwas, rvsf=''):
  gwas = gwas.select('i', 'rsID', 'code1k', 'OR')
  if rvsf == '':
        return gwas
  gwas.write.mode('overwrite').parquet(rvsf)
 



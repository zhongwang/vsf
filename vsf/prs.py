from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.getOrCreate()

def cal_PRS(gvsf, gwas, output=''):
    gwas = gwas.withColumn('index', F.concat_ws('|', 'trait', 'pubmedID'))
    results = (gvsf
               .join(gwas, on=['rsID', 'code'])
               .withColumn('OR', F.col('dose')*F.col('OR'))
               .groupby(['sample_name', 'index'])
               .agg(F.sum('OR').alias('prs_score'))
    )
    # add trait info
    results = ( results
               .join(gwas.select('index', 'trait', 'pubmedID', 'date'), on='index', how='left')
               .drop('index')
               .distinct()
               .sort(['sample_name', 'trait']) 
    )
    if output == '':
        return results
    results.to_pandas_on_spark().to_pandas().to_csv(output, index=False, header=True, sep='\t')
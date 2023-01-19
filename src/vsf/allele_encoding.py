# convert alleles to code
import pyspark.sql.functions as F
genomes = spark.read.parquet('s3a://zhong-active/PGS/1kgenome_dbSNP328.pq')
grc37_p13 = spark.read.parquet('s3a://zhong-active/PGS/dbsnp328/dbsnp328_grc37_p13.pq')

code = (genomes
 .select(
   F.col('ID').astype('long'),
   F.posexplode(F.split(F.concat_ws(',', 'REF', 'ALT'), ',')).alias('code1k', 'allele') 
 )
 .select('ID', F.col('code1k').astype('int'), 'allele')
 .join(grc37_p13.select(F.col('rsID').alias('ID'), 'code', 'allele'), on=['ID', 'allele'])
 .drop('allele')
 )

code.write.mode('overwrite').parquet('s3a://zhong-active/PGS/1kgenome_dbSNP328_code_mapping.pq')
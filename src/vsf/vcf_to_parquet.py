

import pyspark.sql.functions as F

# assuming dbsnp database has been downloaded here
path = 's3a://zhong-active/PGS/dbsnp328/'

dbsnps = [
    {'vcf': path + 'GRCh37.gz', 'out': path + 'dbsnp328_grc37_p13.pq'},
    {'vcf': path + 'GRCh39.gz', 'out': path + 'dbsnp328_grc38_p13.pq'},
]

def dbsnp_vcf_to_parquet(dbsnp, parquet_file, partitions=200):
    vcf = (spark
         .read
         .csv(dbsnp, sep='\t', header=None, comment='#')
         .toDF('chr', 'pos', 'rsID', 'ref', 'alt', 'q1', 'q2', 'annot')
         .select(
           F.regexp_extract('chr', r'NC_0+(\d+).', 1).astype('int').alias('chr'),
           F.col('pos').astype('long'),
           F.regexp_extract('rsID', r'rs(\d+)', 1).astype('long').alias('rsID'),
           F.posexplode(F.split(F.concat_ws(',', 'ref', 'alt'), ',')).alias('code', 'allele')                         
         )
         .drop_duplicates(['chr', 'pos', 'allele', 'rsID'])
         .where(F.col('chr')>0)         
        )
    vcf.repartition(partitions).write.mode('overwrite').parquet(parquet_file)
    
for dbsnp in dbsnps:
    dbsnp_vcf_to_parquet(dbsnp['vcf'], dbsnp['out'])
    
# 1000 genomes on S3
data_path ='s3a://1000genomes/release/20130502/'
prefix = 'ALL.chr'
suffix = '.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz'
chroms = [data_path + prefix + str(c) + suffix for c in range(1, 23)] 
chrY = data_path + 'ALL.chrY.phase3_integrated_v1b.20130502.genotypes.vcf.gz'
chrX = data_path + 'ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf.gz'
chroms = chroms + [chrX, chrY]

# change chromosome# to numerical ID, using GRC37 specification
mapping = {
  'X': '23',
  'Y': '24',
  'M': '25'
}
mapping.update({str(i+1):str(i+1) for i in range(22)})
# other chromosomes will throw out errors later
apply_mapping_udf = F.udf(lambda x: mapping.get(x, x))


def get_vcf_headers(vcf, numHeaderLines=1000):
    """
    column headers of vcf files
    """
    return (spark
            .read
            .csv(vcf, sep='\n', header=None)
            .limit(numHeaderLines)
            .where(F.substring('_c0', 1, 6) == '#CHROM')
            .collect()[0]['_c0'][1:]
            .split('\t')
    )
    
def chr_vcf_to_spark_df(vcfs, limit=0):
    """
    combine a list of vcf files (one per chromosome) into one spark df
    set limit to a small number to debug
    """
    all_vcf = None
    for chrom in vcfs:
 #       print("Adding " + chrom)
        headers = get_vcf_headers(chrom)
        if limit>0:
            vcf_c = spark.read.csv(chrom, comment='#', sep='\t', header=None).limit(limit).toDF(*headers)
        else:
            vcf_c = spark.read.csv(chrom, comment='#', sep='\t', header=None).toDF(*headers)
        # do a count to force read the file, otherwise headers will be changed
 #       print("Adding %d records." % vcf_c.count())
        if all_vcf is None:
            all_vcf = vcf_c
        else:
            all_vcf = all_vcf.unionByName(vcf_c, allowMissingColumns=True)  
    # change letter chromosome IDs to numerical
    print("Totally we added %d records." % all_vcf.count())
    all_vcf = all_vcf.withColumn('CHROM', apply_mapping_udf('CHROM').astype('int'))
    return all_vcf

all_vcf = chr_vcf_to_spark_df(chroms)
all_vcf.repartition(200).write.mode('overwrite').parquet('1kgenome_dbSNP328.pq')  
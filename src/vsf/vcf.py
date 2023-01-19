"""_Convert VCF to VSF_

Support both genotype VCF and dbSNP VCF

TODO:
  - mapping codes
  - adding index for sparse array, or using multi-dimensional array?

"""
from pyspark.sql import SparkSession
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
import pyspark.sql.functions as F
from pyspark.sql.types import *
from vsf.utils import *

spark = SparkSession.builder.getOrCreate()

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

def vcf_to_parquet(output, vcfs, format='gvcf', partitions=200, limit=0):
    """_convert vcf to parquet_

    Args:
        output (_str_): _output parquet file_
        vcfs (_list(str)_): _a list of vcf files_
        format (str, optional): _genotype vcf (gvcf) or dbSNP (dbsnp)_. Defaults to 'gvcf'.
        paritions (int, optional): _number of partitions_. Defaults to 200.
        paritions (int, optional): _number of lines to read_. Defaults to 0, all.

    Returns:
        _none_: _none_
    """
    all_vcf = None
    for vcf in vcfs:
        print("Adding " + vcf)
        if format == 'dbsnp':
            vcf_c =(spark
                .read
                .csv(vcf, sep='\t', header=None, comment='#')
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
            # change letter chromosome IDs to numerical
            vcf_c = vcf_c.withColumn('CHROM', apply_mapping_udf('CHROM').astype('int'))
        elif format == 'gvcf':   
            headers = get_vcf_headers(vcf)
            if limit>0:
                vcf_c = spark.read.csv(vcf, comment='#', sep='\t', header=None).limit(limit).toDF(*headers)
            else:
                vcf_c = spark.read.csv(vcf, comment='#', sep='\t', header=None).toDF(*headers)
        else:
            raise Exception("vcf format: " + format + " is not supported.")

        if all_vcf is None:
            all_vcf = vcf_c
        else:
            all_vcf = all_vcf.unionByName(vcf_c, allowMissingColumns=True)  
        
    print("Totally we added %d records." % all_vcf.count())       
    all_vcf.repartition(partitions).write.mode('overwrite').parquet(output)
    
def update_rsID(dbsnp, in_vcf, out_vcf):
        """_update to latest rsIDs in dbSNP based on chrom and position_

        Args:
            dbsnp (_str_): _dbSNP vnf in parquet_
            in_vcf (_str_): _input vcf in parquet_
            out_vcf (_str_): _output vcf in parquet_
        """

        dbsnp = (spark
        .read
        .parquet(dbsnp)
        .select(F.col('chr').alias('CHROM'), F.col('pos').alias('POS'), F.col('rsID').alias('ID'))
        .drop_duplicates(['CHROM', 'POS'])
        )    

        all_vcf = (in_vcf
                .withColumnRenamed('ID', 'oldID')
                .join(dbsnp, on=['CHROM', 'POS'], how='left')
                )

        all_vcf.write.mode('overwrite').parquet(out_vcf)  
        

def allele_encoding(dbsnp, in_gvcf, code_mappings):
  """_map alleles codes to dbSNP codes_

  Args:
      dbsnp (_type_): _dbSNP parquet file_
      in_gvcf (_type_): _input gVCF parquet file_
      code_mappings (_type_): _a map from gVCF allele to dbSNP allele_
  """
  genomes = spark.read.parquet(in_gvcf)
  dbsnp = spark.read.parquet(dbsnp)

  code = (genomes
  .select(
    F.col('ID').astype('long'),
    F.posexplode(F.split(F.concat_ws(',', 'REF', 'ALT'), ',')).alias('code1k', 'allele') 
  )
  .select('ID', F.col('code1k').astype('int'), 'allele')
  .join(dbsnp.select(F.col('rsID').alias('ID'), 'code', 'allele'), on=['ID', 'allele'])
  .drop('allele')
  )

  code.write.mode('overwrite').parquet(code_mappings)
        
def gvcf_to_vsf(gvcf, vsf):
      """_convert gvcf to vsf_

  Args:
      gvcf (_str_): _gvcf in parquet format_
      vsf (_type_): _vsf in parquet format_
  """
  
  genomes = spark.read.parquet(gvcf)
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
  # this takes a long while
  a1 = IndexedRowMatrix(a1.rdd.map(lambda row: IndexedRow(*row))).toCoordinateMatrix()
  a2 = IndexedRowMatrix(a2.rdd.map(lambda row: IndexedRow(*row))).toCoordinateMatrix()

  a1 = (spark
  .createDataFrame(a1.transpose().entries)
  .where(F.col('value')>0)
  .toDF('sample', 'rsID', 'code')
  )
  a2 = (spark
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
        .withColumn('dose', F.lit(1.0))
        .select('sample', 'rsID', 'code', 'dose')
        .groupby('sample', 'rsID', 'code')
        .agg(F.sum('dose').alias('dose'))
      )

  gt.write.mode('overwrite').parquet(vsf)

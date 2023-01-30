"""_Convert VCF to VSF_

Support both genotype VCF and dbSNP VCF

TODO:
  - other vcf-related functions. filter, select, etc
  - 

"""
from pyspark.sql import SparkSession
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
import pyspark.sql.functions as F
from pyspark.sql.types import *
from vsf.utils import *
from itertools import chain

spark = SparkSession.builder.getOrCreate()

def get_vcf_headers(vcf, numHeaderLines=1000):
    """_read vcf file headers_

    Args:
        vcf (_str_): _input vcf file_
        numHeaderLines (int, optional): _use a larger number for super large headers_. Defaults to 1000.

    Returns:
        _list_: _a list vcf column names_
    """

    return (spark
            .read
            .csv(vcf, sep='\n', header=None)
            .limit(numHeaderLines)
            .where(F.substring('_c0', 1, 6) == '#CHROM')
            .collect()[0]['_c0'][1:]
            .split('\t')
    )

def vcf_to_parquet(vcfs, output='', format='pq', partitions=200, limit=0):
    """_convert vcf to parquet_

    Args:
        vcfs (_list(str)_): _a list of vcf files_
        output (_str_): _output parquet file_, if empty, return dataframe
        format (str, optional): _parquet (pq), genotype vcf (gvcf) or dbSNP (dbsnp)_. Defaults to 'pq'.
        paritions (int, optional): _number of partitions_. Defaults to 200.
        limit (int, optional): _number of lines to read_. Defaults to 0, read all lines.

    Returns:
        _any_: _none or dataframe_
    """
    all_vcf = None
    for vcf in vcfs:
        print("Adding " + vcf)
        if format == 'dbsnp':
            vcf_c =(spark
                .read
                .csv(vcf, sep='\t', header=None, comment='#')
                .repartition(partitions)    
                .toDF('chr', 'pos', 'rsID', 'ref', 'alt', 'q1', 'q2', 'annot')
                .select(
                F.regexp_extract('chr', r'NC_0+(\d+).', 1).alias('chr'), # beware of empty string 
                F.col('pos').astype('long'),
                F.regexp_extract('rsID', r'rs(\d+)', 1).astype('long').alias('rsID'),
                F.posexplode(F.split(F.concat_ws(',', 'ref', 'alt'), ',')).alias('code', 'allele'),
                'annot'                         
                )
                .where(F.col('chr') != '') 
                .drop_duplicates(['rsID', 'code'])  # remove potential duplicated entries                 
                )
            # change letter chromosome IDs to numerical
            vcf_c = vcf_c.withColumn('chr', chr_mapping[vcf_c['chr']])
        elif format == 'gvcf':   
            headers = get_vcf_headers(vcf)
            if limit>0:
                vcf_c = spark.read.csv(vcf, comment='#', sep='\t', header=None).limit(limit).toDF(*headers).repartition(partitions)
            else:
                vcf_c = spark.read.csv(vcf, comment='#', sep='\t', header=None).toDF(*headers).repartition(partitions)
        elif format == 'pq':
            vcf_c = spark.read.parquet(vcf).repartition(partitions) 
        
        else:
            raise Exception("vcf format: " + format + " is not supported.")

        if all_vcf is None:
            all_vcf = vcf_c
        else:
            all_vcf = all_vcf.unionByName(vcf_c, allowMissingColumns=True)  
        
    print("Totally we added %d records." % all_vcf.count())
    if output == '':
        return all_vcf       
    all_vcf.repartition(partitions).write.mode('overwrite').parquet(output)
    
def update_rsID(dbsnp, in_vcf, out_vcf='', keep=False):
    """_update to latest rsIDs in dbSNP based on chrom and position, using assembly-specific dbsnp. The original column 'ID" will be renamed to 'oldID'._

    Args:
        dbsnp (_str_): _dbSNP vnf dataframe_
        in_vcf (_str_): _input vcf dataframe_
        out_vcf (_str_): _output vcf in parquet, if empty, return dataframe_
        keep (_bool_): _whether or not to keep records in gVCF that have no matching rsIDs in dbSNP, default to False, no keeping
    """
    # update column names in dbSNP to match gVCF
    dbsnp = (dbsnp
    .select(F.col('chr').alias('CHROM'), F.col('pos').alias('POS'), F.col('rsID').alias('ID'))
#       .drop_duplicates(['CHROM', 'POS'])
    )    
    if keep: # keep records in gVCF that have no matching rsIDs
        all_vcf = (in_vcf
            .withColumnRenamed('ID', 'oldID')
            .join(dbsnp, on=['CHROM', 'POS'], how='left')
            ) 
    else:   
        all_vcf = (in_vcf
                .withColumnRenamed('ID', 'oldID')
                .join(dbsnp, on=['CHROM', 'POS'], how='inner')
                )
    if out_vcf == '':
        return all_vcf
    all_vcf.write.mode('overwrite').parquet(out_vcf)  
        

def allele_encoding(dbsnp, in_gvcf, code_mappings=''):
    """_map alleles codes in gGVCF to dbSNP codes_
    The result is a mapping from gGVF code('code1k') to dbSNP code ('code'), which later will be used to transform codes back and forth.
    
    Args:
        dbsnp (_dataframe_): _dbSNP dataframe_
        in_gvcf (_dataframe_): _input gVCF dataframe_
        code_mappings (_any_): _file name to store the map from gVCF allele to dbSNP allele, if empty, return the dataframe_
    """

    code = (in_gvcf
    .select(
        F.col('ID').astype('long'),
        F.posexplode(F.split(F.concat_ws(',', 'REF', 'ALT'), ',')).alias('code1k', 'allele') 
    )
    .select('ID', F.col('code1k').astype('int'), 'allele')
    .join(dbsnp.select(F.col('rsID').alias('ID'), F.col('code').astype('int'), 'allele'), on=['ID', 'allele'])
    .drop('allele')
    )
    if code_mappings == '':
        return code
    code.write.mode('overwrite').parquet(code_mappings)
        
def gvcf_to_vsf(gvcf, output='', min_quality=0, filter=False, code_mapping=''):
    """_convert gVCF to sparse gVSF format_
    
    Args:
        gvcf (_dataframe_): _gVCF_
        vsf (str, optional): _output gVSF_. If empty, return the dataframe(Default).
        min_quality (int, optional): _filter out low quality variant below this_. Defaults to 0.
        filter (bool, optional): _whether or not apply a filter for variant with a'PASS' status. Defaults, no.
        code_mapping (str, optional): _code mapping file to transform allele coding_. Defaults to ''.

    Returns:
        _any_: _none or dataframe_
    """
    # code is derived from gvcf
    samples = gvcf.columns[9:-1]
    names = {i:samples[i] for i in range(len(samples))}
    
    # filter the data
    if min_quality > 0:
        gvcf = gvcf.where(F.col('QUAL') >= min_quality)
    if filter:
        gvcf = gvcf.where(F.col('FILTER') == 'PASS')    
    
    # split the two alleles
    a1 = (gvcf
            .select(
            'ID',
            *((F.split(c, '\|').getItem(0).astype('int').alias(c) for c in samples))
            )
            .fillna(0) # assume it is reference allele if missing
            .withColumn('gts', F.array(*samples))
            .select('ID', 'gts')
        )

    a2 = (gvcf
            .select(
            'ID',
            *((F.split(c, '\|').getItem(1).astype('int').alias(c) for c in samples))
            )
            .fillna(0)
            .withColumn('gts', F.array(*samples))
            .select('ID', 'gts')
        )
    # this takes a long while
    # convert genotype data to coordinated matrix
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
            .select('sample', 'rsID', F.col('code').astype('int'), 'dose')
            .groupby('sample', 'rsID', 'code')
            .agg(F.sum('dose').alias('dose'))
        )
    
    
    if code_mapping != '':
        gt = (gt
                .withColumnRenamed('code', 'code1k')
                .join(code_mapping.select(F.col('ID').alias('rsID'), 'code', 'code1k'), on=['rsID', 'code1k'], how='left')
                .drop('code1k')
                .fillna({'code':0})
        )
        
    # after the conversion, sample becomes index to the original samples, let's add the name back
    sample_mapping = F.create_map([F.lit(x) for x in chain(*names.items())])
    gt = gt.withColumn('sample_name', sample_mapping[gt['sample']] ) 
        
    if output == '':
        return gt
    
    gt.write.mode('overwrite').parquet(output)

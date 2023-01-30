"""Test cases for the __main__ module."""
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
spark = SparkSession.builder.getOrCreate()

sys.path.append('../')
from vsf.gwas import *
from vsf.vcf import *
from vsf.prs import *

# small testing datasets, no need to use intermediate files
prefix = 'datasets'
dbsnp_test_file = prefix + '/dbsnp_test.csv.gz'
gvcf_test_file = prefix + '/gvcf_test.vcf.gz'
gwas_test_file = prefix + '/gwas_test.csv.gz'

@pytest.fixture
def test_dbsnp_to_parquet(dbsnp_test_file):
    """ return a dbsnp dataframe """
    # read dbsnp
    dbsnp = vcf_to_parquet([dbsnp_test_file], format='dbsnp', partitions=2)
  
    return dbsnp

@pytest.fixture    
def test_gvcf_to_parquet(gvcf_test_file):
    """    return a gVCF dataframe  """
    # read genotype file
    gvcf = vcf_to_parquet([gvcf_test_file], format='gvcf', partitions=2)

    return gvcf
     
    
def test_prs(dbsnp, gvcf, gwas):
   # update rsIDs in gvcf using dbSNP
    gvcf = update_rsID(dbsnp, in_vcf=gvcf)
    # convert vcf to sparse format vsf
    gvsf = gvcf_to_vsf(gvcf)    
    # read gwas
    gwas = gwas_csv_to_parquet(gwas_test_file)
    gwas = gwas_fill_rsID(dbsnp, gwas)

    # mapping code to gvcf
    code_mapping = allele_encoding(dbsnp, in_gvcf=gvcf)
    gwas = gwas_add_code(dbsnp, gwas, code_mapping=code_mapping)
    # calculate PRS
    results = cal_PRS(gvsf, gwas)
    count = results.where((F.col('sample_name') == 'HG00096') & (F.col('pubmedID') == 25763902) & (F.col('prs_score') > 1.0)).count()
    assert count == 1


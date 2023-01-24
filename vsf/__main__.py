"""Command-line interface."""
import os
import sys

sys.path.append('../')
import click
from vsf.gwas import *
from vsf.vcf import *
# testing datasets
prefix = 'dbfs:/mnt/zhong-active/PGS'
dbsnp_test_file = prefix + '/dbsnp_test.csv.gz'
gvcf_test_file = prefix + '/gvcf_test.vcf.gz'
gwas_test_file = prefix + '/gwas_test.csv.gz'


@click.command()
@click.version_option()
def main() -> None:
    """VariantSparseFormat."""
    # read dbsnp
    dbsnp = vcf_to_parquet([dbsnp_test_file], format='dbsnp', partitions=2)
    dbsnp.show(2)
    # read genotype file
    gvcf = vcf_to_parquet([gvcf_test_file], format='gvcf', partitions=2)
    gvcf.show(2)
    # update rsIDs in gvcf using dbSNP
    gvcf = update_rsID(dbsnp, in_vcf=gvcf)
    gvcf.show(2)
    # convert vcf to sparse format vsf
    gvsf = gvcf_to_vsf(gvcf)
    gvsf.show(2)
    # read gwas
    gwas = gwas_csv_to_parquet(gwas_test_file)
    gwas.show(2)
    gwas = gwas_fill_rsID(dbsnp, gwas)
    gwas.show(2)
    # mapping code to gvcf
    codemapping = allele_encoding(dbsnp, in_gvcf=gvcf)
    codemapping.show(2)
    gwas = gwas_add_code(dbsnp, gwas, codemapping)
    gwas.show(2)

if __name__ == "__main__":
    main(prog_name="vsf")  # pragma: no cover

"""Command-line interface."""

import click
from vsf.gwas import *
from vsf.vcf import *
from vsf.prs import *

# small testing datasets, no need to use intermediate files
prefix = 's3://share.jgi-ga.org/vsf/test'
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
    code_mapping = allele_encoding(dbsnp, in_gvcf=gvcf)
    code_mapping.show(2)
    gwas = gwas_add_code(dbsnp, gwas, code_mapping=code_mapping)
    gwas.show(2)
    # calculate PRS
    results = cal_PRS(gvsf, gwas)
    results.sort('sample_name', 'trait').show()

if __name__ == "__main__":
    main(prog_name="vsf")  # pragma: no cover

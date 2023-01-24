"""Command-line interface."""
import click
from gwas import *
from vcf import *
# testing datasets
dbsnp_test_file = '../../tests/datasets/dbsnp_test.csv.gz'
gvcf_test_file = '../../tests/datasets/gvcf_test.vcf.gz'
gwas_test_file = '../../tests/datasets/gwas_test.csv.gz'


@click.command()
@click.version_option()
def main() -> None:
    """VariantSparseFormat."""
    # read dbsnp
    dbsnp = vcf_to_parquet([dbsnp_test_file], format='dbsnp', partitions=2)
    # read genotype file
    gvcf = vcf_to_parquet([gvcf_test_file], format='gvcf', partitions=2)
    # update rsIDs in gvcf using dbSNP
    gvcf = update_rsID(dbsnp, in_vcf=gvcf)
    # convert vcf to sparse format vsf
    gvsf = gvcf_to_vsf(gvcf)
    # read gwas
    gwas = gwas_csv_to_parquet(gwas_test_file)
    gwas = gwas_fill_rsID(dbsnp, gwas)
    # mapping code to gvcf
    codemapping = allele_encoding(dbsnp, in_gvcf=gvcf)
    gwas = gwas_add_code(dbsnp, gwas, codemapping)

if __name__ == "__main__":
    main(prog_name="vsf")  # pragma: no cover

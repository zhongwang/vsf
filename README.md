# Variant Sparse Fromat (VSF) Specifications

  - Column-based
  - Sparse-format
  - Sample-specification and reference-specification

## 1. Genotype file

  1. rsID: INT, unique IDs from dbSNP
  2. alleleID:INT, numerical encoded alleles from dbSNP, first allele is 0
  3. sampleID: INT, sample/individual ID
  4. alleleDosage: INT, 0, 1, 2
  5. allele: STRING, allele sequence
  6. OtherAnnotations: ANY, optional

## 2. GWAS result file  

  1. rsID: INT, unique IDs from dbSNP
  2. alleleID:INT, numerical encoded alleles from dbSNP, first allele is 0
  3. traitID: INT, phenotype/trait ID
  4. OR: FLOAT, GWAS reported Odd Ratio
  5. trait: STRING, trait name
  6. OtherAnnotations: ANY, optional
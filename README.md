# VariantSparseFormat

[![PyPI](https://img.shields.io/pypi/v/vsf.svg)][pypi_]
[![Status](https://img.shields.io/pypi/status/vsf.svg)][status]
[![Python Version](https://img.shields.io/pypi/pyversions/vsf)][python version]
[![License](https://img.shields.io/pypi/l/vsf)][license]

[![Read the documentation at https://vsf.readthedocs.io/](https://img.shields.io/readthedocs/vsf/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Tests](https://github.com/zhongwang/vsf/workflows/Tests/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/zhongwang/vsf/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi_]: https://pypi.org/project/vsf/
[status]: https://pypi.org/project/vsf/
[python version]: https://pypi.org/project/vsf
[read the docs]: https://vsf.readthedocs.io/
[tests]: https://github.com/zhongwang/vsf/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/zhongwang/vsf
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

## Features

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
- TODO

## Requirements

- TODO

## Installation

## Install pySpark on a big memory node
To install, first install conda and create a new environment. In the new environment:
```
pip install pyspark # this installs the latest version (pyspark 3.1.2)
#conda install pyspark # this installs an older version
```
Then install jupyter if you want to use the notebook, and add the following to your .bashrc to set $SPARK_HOME:

```
export PYSPARK_PYTHON=python3 
export SPARK_HOME=~/.conda/envs/pyspark/lib/python3.10/site-packages/pyspark
```
To use AWS S3:
```
wget https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk-bundle/1.12.357/aws-java-sdk-bundle-1.12.357.jar -O {SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.357.jar
wget https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -O {SPARK_HOME}/jars/hadoop-aws-3.3.2.jar
```
In a jupyter notebook, use the following to set up access to S3:
```
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyspark

aws_access_key_id = 'XXXX'
aws_secret_access_key = 'XXX'

spark = (SparkSession 
            .builder 
            .appName("1kgenome") 
            .master('local[72]')
            .config('spark.executor.heartbeatInterval', '1000s')
            .config('spark.network.timeout', '10000s')
            .config('spark.executor.memory', '4000G')
            .config('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
            .config('spark.driver.extraJavaOptions', '-Dcom.amazonaws.services.s3.enableV4=true')
            .config('spark.kryoserializer.buffer.max', '1G') # higher causes oom （GC limit）
            .config('spark.local.dir','/mnt/data/spark') # /tmp might be full
            .config('spark.sql.autoBroadcastJoinThreshold', '-1')
            .getOrCreate()
        )
sc = spark.sparkContext
sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key_id)
hadoopConf.set('fs.s3a.secret.key', aws_secret_access_key)

```

To use PMem, 
```
mm -c jupyter.yml jupyter-notebook
```

You can install _VariantSparseFormat_ via [pip] from [PyPI]:

```console
$ pip install vsf
```

## Usage

Please see the [Command-line Reference] for details.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_VariantSparseFormat_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [@cjolowicz]'s [Hypermodern Python Cookiecutter] template.

[@cjolowicz]: https://github.com/cjolowicz
[pypi]: https://pypi.org/
[hypermodern python cookiecutter]: https://github.com/cjolowicz/cookiecutter-hypermodern-python
[file an issue]: https://github.com/zhongwang/vsf/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/zhongwang/vsf/blob/main/LICENSE
[contributor guide]: https://github.com/zhongwang/vsf/blob/main/CONTRIBUTING.md
[command-line reference]: https://vsf.readthedocs.io/en/latest/usage.html

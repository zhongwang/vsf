import pyspark.sql.functions as F
from pyspark.sql.types import *
from itertools import chain


# replace chromosome numbering
mapping = {
  'X': 23,
  'Y': 24,
  'M': 25
}
mapping.update({str(i+1):(i+1) for i in range(22)})
chr_mapping = F.create_map([F.lit(x) for x in chain(*mapping.items())])
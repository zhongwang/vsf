import pyspark.sql.functions as F


# replace chromosome numbering
mapping = {
  'X': '23',
  'Y': '24',
  'M': '25'
}
mapping.update({str(i+1):str(i+1) for i in range(22)})
apply_mapping_udf = F.udf(lambda x: mapping.get(x, x))
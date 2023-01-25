# this doesn't work, we use pandas/numpy

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.getOrCreate()

def multiply_matrices(A, B, enable_broadcast=False):
    # BASED ON: https://github.com/Fokko/spark-matrix-multiplication/blob/master/multiply.py
    # https://notes.mindprince.in/2013/06/07/sparse-matrix-multiplication-using-sql.html
    A.registerTempTable("A")

    if enable_broadcast:
        # if matrix is small auto broadcast will do its job
        B = F.broadcast(B)

    B.registerTempTable("B")
    return spark.sql("""
    SELECT
        A.numpy_row numpy_row,
        B.numpy_column numpy_column,
        SUM(A.data_value * B.data_value) data_value
    FROM
        A
    JOIN B ON A.numpy_column = B.numpy_row
    GROUP BY A.numpy_row, B.numpy_column
    """)

def cal_PRS(gvsf, rvsf, output='', broadcast=False):
    
    # index relevant rsID-code
    index = (rvsf
        .select('rsID', 'code')
        .drop_duplicates()
        .rdd
        .zipWithIndex()
        .map(lambda x: (x[1], x[0][0], x[0][1]))
        .toDF(['j', 'rsID', 'code'])
        )
    gvsf = gvsf.join(index, on=['rsID', 'code'], how='inner')
    rvsf = rvsf.join(index, on=['rsID', 'code'], how='inner')
    
    
    rvsf_matrix = rvsf.select(F.col('i').alias('numpy_row'), F.col('j').alias('numpy_column'), F.col('OR').alias('data_value'))
    gvsf_matrix = gvsf.select(F.col('sample').alias('numpy_row'), F.col('j').alias('numpy_column'), F.col('dose').alias('data_value'))
    results = multiply_matrices(gvsf_matrix, rvsf_matrix, enable_broadcast=broadcast).toDF('sample', 'trait', 'prs_score')
    if output == '':
        return results
    results.to_pandas_on_spark().to_pandas().to_csv(output, index=False, header=True, sep='\t')
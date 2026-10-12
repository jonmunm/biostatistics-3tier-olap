from pyspark.sql import functions as F
from pyspark.sql.column import Column

def clean_text(column:str, default:str = "999999") -> Column:
    tfm = F.upper(
        F.trim(
            F.regexp_replace(F.col(column), "[.,]", "")
        )
    )

    return F.when(
        (tfm == "") | (tfm.isNull()),
        default
    ).otherwise(
        tfm
    ).alias(column)

def fill_null_integers(column:str, default:int = 999999) -> Column:
    return F.coalesce(F.col(column), F.lit(default)).alias(column)

def fill_null_integers_and_impute(column:str, default:int, impute:int) -> Column:
    exp = F.coalesce(F.col(column), F.lit(default))
    return F.when(exp == F.lit(impute), F.lit(default))\
        .otherwise(exp)\
        .alias(column)

def extract_year(source_column:str, new_column:str) -> Column:
    col = F.col(source_column)
    return F.year(
        F.coalesce(
            F.to_date(col, 'M/d/yyyy'),
            F.to_date(col, 'MM/dd/yyyy'),
            F.to_date(col, 'MM-dd-yyyy'),
            F.to_date(col, 'd-M-yyyy'),
            F.to_date(col, 'dd-M-yyyy'),
            F.to_date(col, 'dd-MM-yyyy'),
            F.to_date(col, 'dd/MM/yyyy'),
            F.to_date(col, 'yyyy-MM-dd')
        )
    ).alias(new_column)
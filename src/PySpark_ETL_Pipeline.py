
import os
import sys
from datetime import datetime


# Set these BEFORE importing SparkSession
os.environ['HADOOP_HOME']           = r'C:\hadoop'
os.environ['JAVA_HOME']             = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.31.11-hotspot'
os.environ['PATH']                  = r'C:\hadoop\bin;' + os.environ['PATH']
os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *


# ── SparkSession ──────────
def get_spark():
    spark=SparkSession.builder.appName('ETL_Banking_Pipeline').\
                            master('local[*]').\
                            config('spark.sql.shuffle.partitions','8').\
                            config('spark.driver.memory','2g').\
                            config('spark.sql.legacy.TimeParserPolicy','LEGACY').\
                            getOrCreate()
    return spark


# ── EXTRACT ───────────────────────────
"""
Read the raw banking CSV into a Spark DataFrame.
inferSchema=True handles the mixed types (string, double, int, date).
"""
def extract(spark,path):
    print("\n[EXTRACT] Reading CSV...")
    df_from_csv=spark.read.csv(path,
                               header=True,
                               inferSchema=True)
    print(f'[EXTRACT] {df_from_csv.count()} rows & {len(df_from_csv.columns)} Columns Extracted')
    #print(df_from_csv.printSchema())
    #print(df_from_csv.show(5))

    return df_from_csv


# ── VALIDATE ──────────────────
def validate(df, report_path: str):
    """
    Run data quality checks against the raw dataset.
    Writes a plain-text report to disk and prints a summary.

    Checks:
    1.  Null counts per column
    2.  Negative transaction amounts
    3.  Invalid transaction_direction values (must be DEBIT / CREDIT)
    4.  Invalid transaction_status values
    5.  Fraud flag values outside 0/1
    6.  Credit score out of range (300–850)
    7.  Duplicate transaction_ids
    """
    print("\n[VALIDATE] Running data quality checks...")

    total_rows = df.count()
    issues=[]
    lines = ["="*20,"DATA QUALITY REPORT",f'Generated : {datetime.now():%Y-%m-%d %H-%M-%S}',f"Total Rows : {total_rows}","="*20]
    
    #1.  Null counts per column
    lines.append('\n── Null Count ────────────────')
    null_count = df.select([F.sum(
                            F.col(c).isNull().cast('int')).alias(c)
                            for c in df.columns]).collect()[0].asDict()
    
    for col,cnt in null_count.items():
        if cnt > 0:
            pct = cnt/total_rows * 100
            line = f" {col:<30} {cnt:>8,} ({pct:.1f}%)"
            lines.append(line)
            issues.append(f'Nulls in {col} : {cnt:,}')
    
    if not any(v>0 for v in null_count.values()):
        lines.append('No Null Values found')

    # 2. Negative transaction amounts
    lines.append(f"\n── NEGATIVE AMOUNTS ────────────────")
    Negative_count = df.filter(F.col('transaction_amount') < 0).count()
    lines.append(f"Negative Amounts : {Negative_count}")
    if Negative_count > 0:
        issues.append(f"Negative Amounts : {Negative_count}")
    
    # 3. Invalid transaction_direction
    lines.append(f"\n── INVALID TRANSACTION DIRECTION ────────────────")
    valid_trans_dir = ['Debit','Credit']
    invalid_trans_dir_count = df.filter(~F.col('transaction_direction').isin(valid_trans_dir)).count()
    lines.append(f'{invalid_trans_dir_count} Amounts have invalid transaction Direction')
    if invalid_trans_dir_count > 0:
        issues.append(f'{invalid_trans_dir_count} Amounts have invalid transaction Direction')

    # 4. Transaction_status values
    lines.append("\n── TRANSACTION STATUS VALUES ────────────────")
    status_counts=df.groupby(F.col('transaction_status')).count().collect()
    for row in status_counts:
        lines.append(f"{row['transaction_status']:<20} {row['count']}")
    
    # 5. Fraud flag outside 0/1
    lines.append("\n── FRAUD FLAG DISTRIBUTION ──────────────────")
    fraud_flag_count=df.groupBy(F.col('is_fraud')).count().orderBy(F.col('is_fraud')).collect()
    for row in fraud_flag_count:
        lines.append(f"{row['is_fraud']:<20} {row['count']}")

    invalid_fraud_flag_count = df.filter(~F.col('is_fraud').isin([0,1])).count()
    if invalid_fraud_flag_count > 0:
        lines.append(f'Invalid is_fraud values: {invalid_fraud_flag_count}')

    # 6. Credit score range
    lines.append("\n── CREDIT SCORE RANGE ───────────────────────")
    out_of_range = df.filter((F.col('credit_score')< 300) | (F.col('credit_score')> 850)).count()
    lines.append(f"Out-of-range credit scores (< 300 or > 850) : {out_of_range}")
    if out_of_range > 0:
        issues.append(f'No of Transactions with credit Score out of range is : {out_of_range}')

    # 7. Duplicate transaction IDs
    lines.append("\n── DUPLICATE TRANSACTION IDs ────────────────")
    total_ids = df.count()
    distinct_id = df.select(F.col('transaction_id')).distinct().count()
    dups = total_ids - distinct_id
    lines.append(f"  Duplicate transaction_ids : {dups:,}")
    if dups > 0:
        issues.append(f"Duplicate txn IDs: {dups}")

    # Overall status
    lines.append("\n" + "=" * 60)
    if issues:
        lines.append(f"OVERALL STATUS : FAIL — {len(issues)} issue(s) found")
        for i in issues:
            lines.append(f"  .{i}")
    else:
        lines.append("OVERALL STATUS : PASS")
    lines.append("=" * 60)   

    # Write report

    with open(report_path,"w",encoding="utf-8") as f:
        f.write("\n".join(lines))

    # Print summary
    print(f"[VALIDATE] {'FAIL' if issues else 'PASS'} - "
          f"{len(issues)} issue(s). Full Report -> {report_path}")

    return df



def transform(df):
    """
    Clean and enrich the raw dataset.
 
    Steps:
    1.  Standardise column casing (UPPER for categoricals)
    2.  Add time-based columns (year, month, quarter, hour_band)
    3.  Add amount_usd (Amount*0.27)
    4.  Add risk_score (composite: fraud flag + credit score + balance)
    5.  Add risk_label (HIGH / MEDIUM / LOW)
    6.  Add is_large_transaction (top 5% threshold)
    7.  Add days_since_transaction (from today)
    8.  Add lag_amount — previous transaction amount per customer
    9.  Add amount_delta — difference from previous transaction
    10. Filter to COMPLETED transactions only for main output
    """
    print("\n[TRANSFORM] Enriching dataset...")

    # ── 1. Standardise categoricals ────────────────────
    df = df.withColumn("transaction_direction",F.upper(F.col("transaction_direction"))).\
            withColumn("transaction_status",F.upper(F.col("transaction_status"))).\
            withColumn("account_type",F.upper(F.col("account_type"))).\
            withColumn("kyc_status",F.upper(F.col("kyc_status")))
    
    # ── 2. Time-based columns ───────────────────────────────────────────────

    df=df.withColumn("txn_year",F.year('transaction_date')).\
          withColumn("txn_month",F.month('transaction_date')).\
          withColumn("txn_quarter",F.quarter('transaction_date')).\
          withColumn("txn_day_of_week",F.dayofweek('transaction_date')).\
          withColumn("is_weekend",F.col('txn_day_of_week').isin([1,7])).\
          withColumn("hour_band",
                     F.when(F.col("transaction_hour").between(0,5),"NIGHT").\
                       when(F.col("transaction_hour").between(6,11),"MORNING").\
                       when(F.col("transaction_hour").between(12,17),"AFTERNOON").\
                       otherwise("EVENING"))

    # ── 3. Amount in USD ────────────────────────────────────────────────────
    df = df.withColumn("amount_usd",F.round(F.col("transaction_amount")*0.27,2))

    # ── 4 & 5. Risk score + label ────────────────────────────────────────────
    """ Risk logic:
        Base score starts at 0
        +40 if is_fraud = 1
        +20 if credit_score < 580 (poor credit)
        +20 if account_balance < 500 (near zero balance)
        +10 if kyc_status != VERIFIED
        +10 if transaction_amount > 50,000 """
    
    df=df.withColumn("risk_score",
                    F.col("is_fraud") * 40 +
                    F.when(F.col("credit_score") < 580, 20).otherwise(0) +
                    F.when(F.col("account_balance") < 500, 20).otherwise(0) +
                    F.when(F.col("kyc_status") != 'VERIFIED', 10).otherwise(0) +
                    F.when(F.col("transaction_amount") > 50000, 10).otherwise(0)).\
            withColumn("risk_label",
                    F.when(F.col("risk_score") >= 60, "HIGH").\
                      when(F.col("risk_score") >= 30, "MEDIUM").\
                      otherwise("LOW"))
    
    # ── 6. Large transaction flag (top 5% by amount) ────────────────────────
    percentile_95 = df.approxQuantile("transaction_amount",[0.95],0.01)[0]
    df = df.withColumn("is_large_transaction",F.col("transaction_amount") >= percentile_95)

    # ── 7. Days since transaction ────────────────────────────────────────────
    df = df.withColumn(
        "days_since_txn",
        F.date_diff(F.current_date(),F.col("transaction_date")))
    
    # ── 8 & 9. LAG amount + delta per customer (fraud detection pattern) ─────
    customer_window = Window.partitionBy("customer_id").orderBy("transaction_date", "transaction_time")

    df=df.withColumn("previous_amount",
                     F.lag(F.col("transaction_amount")).over(customer_window)).\
          withColumn("amount_delta",
                     (F.col("transaction_amount")-F.col("previous_amount"))).\
          withColumn("spike_flag",
                     F.when((F.col("transaction_amount")) > F.col("previous_amount")*3,True).
                            otherwise(False))
    
    # ── 10. Filter to COMPLETED only for main clean output ───────────────────
    df.show(5)
    clean = df.filter(F.col("transaction_status") == 'SUCCESS')
    before = df.count()
    after = clean.count()

    print(f"[TRANSFORM] {before} raw → {after} COMPLETED rows")
    print(f"[TRANSFORM] Enriched with {len(clean.columns)} columns (was 20)")

    return clean, df   # return both: clean for load, df (full) for fraud alerts

# ── AGGREGATE ─────────────────────────────────────────────────────────────────
def aggregate(clean):
    """
    Produce two summary datasets:

    1. monthly_summary  — total amount by month × channel × direction
    2. customer_risk    — per-customer risk profile with transaction history
    """
    print("\n[AGGREGATE] Building summaries...")

    # ── Monthly channel summary ───────────────────────────────────────────────
    monthly_summary = clean.\
                      groupBy("txn_year","txn_month", "channel", "transaction_direction").\
                      agg(F.sum(F.col("transaction_amount")).alias("total_amount"),
                          F.count(F.col("transaction_amount")).alias("total_count"),
                          F.round(F.avg(F.col("transaction_amount")),2).alias("average_amount"),
                          F.max(F.col("transaction_amount")).alias("max_amount"),
                          F.sum(F.col("is_fraud")).cast("int").alias("fraud_count")).\
                      orderBy("txn_year","txn_month", "channel")
    print(f"[AGGREGATE] Monthly summary: {monthly_summary.count()} rows")
    monthly_summary.show(5,truncate=False)

    # ── Customer risk profile ─────────────────────────────────────────────────
    risk_profile = clean.groupBy("customer_id").\
            agg(
            F.count("transaction_id").alias("total_transactions"),
            F.round(F.sum("transaction_amount"), 2).alias("total_amount"),
            F.round(F.avg("transaction_amount"), 2).alias("avg_amount"),
            F.max("transaction_amount").alias("max_single_txn"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_incidents"),
            F.round(F.avg("credit_score"), 0).alias("avg_credit_score"),
            F.round(F.avg("account_balance"), 2).alias("avg_balance"),
            F.max("risk_score").alias("max_risk_score"),
            F.sum(F.col("spike_flag").cast("int")).alias("spike_count"),
            F.countDistinct("merchant_category").alias("unique_merchants"),
            F.countDistinct("state").alias("unique_states"),
            F.max("transaction_date").alias("last_txn_date")
                ).\
            withColumn("customer_risk_tier",
            F.when(
                (F.col("fraud_incidents") > 0) | (F.col("max_risk_score") >= 60),
                "HIGH")
            .when(
                (F.col("spike_count") > 2) | (F.col("avg_credit_score") < 580),
                "MEDIUM")
            .otherwise("LOW")).\
            orderBy(F.col("fraud_incidents").desc(), F.col("max_risk_score").desc())      
    print(f"[AGGREGATE] Customer risk profiles: {risk_profile.count()} customers")
    risk_profile.groupBy("customer_risk_tier").count().orderBy("customer_risk_tier").show()

    return monthly_summary, risk_profile       


# ── FRAUD ALERTS ──────────────────────────────────────────────────────────────
def extract_fraud_alerts(df_full):
    """
    Extract high-risk transactions from the FULL dataset (not just COMPLETED).
    Flags:
    - is_fraud = 1
    - spike_flag = True (amount > 3× previous for same customer)
    - HIGH risk_label
    - Unverified KYC with large amount
    """
    print("\n[FRAUD ALERTS] Extracting flagged transactions...")

    fraud_alerts = df_full.filter(
        (F.col("is_fraud") == 1) |
        (F.col("spike_flag") == True) |
        (F.col("risk_label") == "HIGH") |
        (
            (F.col("kyc_status") != "VERIFIED") &
            (F.col("transaction_amount") > 50000)
        )
    ).select(
        "transaction_id",
        "customer_id",
        "transaction_date",
        "transaction_amount",
        "transaction_direction",
        "transaction_status",
        "merchant_category",
        "state",
        "channel",
        "credit_score",
        "account_balance",
        "kyc_status",
        "is_fraud",
        "risk_score",
        "risk_label",
        "spike_flag",
        "previous_amount",
        "amount_delta"
    ).orderBy(F.col("risk_score").desc())

    alert_count = fraud_alerts.count()
    fraud_rate  = df_full.filter(F.col("is_fraud") == 1).count()
    print(f"[FRAUD ALERTS] {alert_count:,} flagged transactions")
    print(f"[FRAUD ALERTS] Confirmed fraud (is_fraud=1): {fraud_rate:,}")

    # Show breakdown by risk label
    fraud_alerts.groupBy("risk_label").count().orderBy("risk_label").show()

    return fraud_alerts

# ── LOAD ──────────────────────────────────────────────────────────────────────
def load(clean, fraud_alerts, monthly_summary, customer_risk,output_path):
    """
    Write all outputs to disk.

    clean_transactions  → Parquet, partitioned by state (fast regional queries)
    fraud_alerts        → Parquet, partitioned by risk_label
    monthly_summary     → CSV (single file — easy to open in Excel)
    customer_risk       → CSV (single file)
    """
    print("\n[LOAD] Writing outputs...")

    # Clean transactions — Parquet partitioned by state
    clean.write \
        .mode("overwrite") \
        .partitionBy("state") \
        .parquet(os.path.join(output_path,"clean_transactions"))
    print("[LOAD] ✅ clean_transactions → Parquet (partitioned by state)")

    # Fraud alerts — Parquet partitioned by risk_label
    fraud_alerts.write \
        .mode("overwrite") \
        .partitionBy("risk_label") \
        .parquet(os.path.join(output_path,"fraud_alerts"))
    print("[LOAD] ✅ fraud_alerts → Parquet (partitioned by risk_label)")

    # Monthly summary — single CSV
    monthly_summary.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(os.path.join(output_path,"monthly_summary"))
    print("[LOAD] ✅ monthly_summary → CSV")

    # Customer risk profile — single CSV
    customer_risk.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(os.path.join(output_path,"customer_risk_profile"))
    print("[LOAD] ✅ customer_risk_profile → CSV")


# ── RUN SUMMARY ───────────────────────────────────────────────────────────────
def print_run_summary(start, df_raw, clean, fraud_alerts):
    elapsed = (datetime.now() - start).seconds
    print("\n" + "=" * 60)
    print("ETL RUN SUMMARY")
    print("=" * 60)
    print(f"  Raw rows extracted     : {df_raw.count():>10,}")
    print(f"  Clean rows loaded      : {clean.count():>10,}")
    print(f"  Fraud alerts generated : {fraud_alerts.count():>10,}")
    print(f"  Total runtime          : {elapsed}s")
    print(f"  Output folder          : output/")
    print("=" * 60)
    print("\nOutputs:")
    print("  output/clean_transactions/    ← Parquet, partitioned by state")
    print("  output/fraud_alerts/          ← Parquet, partitioned by risk_label")
    print("  output/monthly_summary/       ← CSV")
    print("  output/customer_risk_profile/ ← CSV")
    print("  output/data_quality_report.txt")


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
def run(path:str):
    start = datetime.now()
    spark        = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    df_raw = extract(spark,path)
    df_raw = validate(df_raw,report_path)
    df_clean, df_full = transform(df_raw)
    mon_summ, risk_prof = aggregate(df_clean)
    fraud_alerts = extract_fraud_alerts(df_full)
    load(df_clean, fraud_alerts, mon_summ, risk_prof,output_path)
    print_run_summary(start, df_raw, df_clean, fraud_alerts)

    spark.stop()
    print(f"\nPySpark Banking ETL — DONE ✅")
    

if __name__ == '__main__':
    path = os.path.join(os.getcwd(),'..','data\indian_banking_transactions.csv')
    
    output_dir = os.path.join("..", "output")
    os.makedirs(output_dir,exist_ok=True)
    
    output_path = os.path.join(os.getcwd(),'..','output')
    report_path = os.path.join(output_dir, "report.txt")
    
    run(path)




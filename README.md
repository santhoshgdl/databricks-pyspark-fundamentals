# Databricks & PySpark Fundamentals

<p align="center">
  <strong>Learning Apache Spark and PySpark through hands-on practice with Databricks.</strong>
</p>

## About This Repository

This repository documents my hands-on learning and implementation of **Apache Spark and PySpark using Databricks**.

It focuses on understanding distributed data processing through practical exercises, working with DataFrames, transforming and cleaning datasets, performing aggregations, joining multiple datasets, implementing window functions, and managing data using different storage formats.

The goal is to build a strong foundation in PySpark and develop practical skills applicable to modern Data Engineering workflows.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Programming language |
| Apache Spark | Distributed data processing |
| PySpark | Python API for Apache Spark |
| Databricks | Development and execution environment |
| Spark SQL | SQL-based data processing |
| Unity Catalog | Data organization and governance |
| CSV / JSON | Source data formats |
| Parquet | Columnar data storage |
| Delta Lake | Transactional data storage |
| Git & GitHub | Version control |

## Repository Structure

```text
databricks-pyspark-fundamentals/
│
├── README.md
├── LICENSE
│
├── notebooks/
│   └── 01_spark_basics.py
│
└── data/
    └── raw/
        ├── BigMart Sales.csv
        └── drivers.json
```

*The notebook filename and dataset names above describe the current learning material; adjust them if the actual filenames in the repository differ.*

## Topics Covered

### 01. Data Reading and Schema Definition

- Reading CSV files using Spark.
- Reading JSON files.
- Schema inference.
- Defining schemas using DDL.
- Defining schemas using `StructType` and `StructField`.
- Inspecting DataFrame schemas.
- Understanding data types.

### 02. DataFrame Operations

- `select()`
- `alias()`
- `filter()`
- `withColumn()`
- `withColumnRenamed()`
- `sort()`
- `orderBy()`
- `limit()`
- `drop()`
- `dropDuplicates()`

### 03. Data Transformations

- Column expressions.
- Adding literal columns using `lit()`.
- Type casting.
- Regular expressions using `regexp_replace()`.
- Conditional transformations using `when()` and `otherwise()`.

### 04. Data Cleaning

- Handling NULL values.
- `dropna()`
- `fillna()`
- Removing duplicate records.
- Data type conversion.
- Replacing inconsistent values.

### 05. String Functions

- `upper()`
- `lower()`
- `initcap()`
- `split()`
- `explode()`
- `array_contains()`

### 06. Date Functions

- `current_date()`
- `date_add()`
- `date_sub()`
- `datediff()`
- `date_format()`

### 07. Aggregations

- `groupBy()`
- `sum()`
- `avg()`
- `collect_list()`
- `pivot()`

### 08. Combining DataFrames

- `union()`
- `unionByName()`
- Inner Join.
- Left Join.
- Right Join.
- Left Anti Join.

### 09. Window Functions

- Window specifications.
- `row_number()`
- `rank()`
- `dense_rank()`
- Cumulative aggregations.
- Window ordering.
- Window frame specifications.

### 10. User Defined Functions (UDF)

- Creating Python functions.
- Registering PySpark UDFs.
- Applying UDFs to DataFrame columns.

### 11. Data Writing and Storage

- Writing DataFrames into CSV.
- Writing DataFrames into Parquet.
- Understanding Delta format.
- Append mode.
- Overwrite mode.
- Error / ErrorIfExists mode.
- Ignore mode.
- Saving DataFrames as tables.
- Working with Unity Catalog Volumes.

### 12. Spark SQL and Views

- Creating temporary views.
- Querying DataFrames using Spark SQL.
- Exploring catalogs and schemas.
- Understanding SQL views.
- Accessing DataFrames through SQL queries.

---

## Datasets

The exercises use the following datasets:

| Dataset | Format | Usage |
|---|---|---|
| BigMart Sales | CSV | DataFrame operations, filtering, transformations and aggregations |
| Drivers | JSON | JSON ingestion and schema exploration |

### Databricks Storage Configuration

The notebooks use Unity Catalog Volumes to access datasets.

Example volume path:

```text
/Volumes/workspace/default/raw_files/
```

Expected file paths:

```text
/Volumes/workspace/default/raw_files/BigMart Sales.csv

/Volumes/workspace/default/raw_files/drivers.json
```

Upload the datasets into your own Unity Catalog Volume and update the paths according to your Databricks environment.

---

## Getting Started

### Prerequisites

- Basic Python knowledge.
- Basic SQL knowledge.
- A Databricks workspace.
- Access to compatible Spark compute.
- Required datasets.

### Running in Databricks

1. Clone or download this repository.
2. Open your Databricks workspace.
3. Import the notebook from the `notebooks/` directory.
4. Upload the required datasets into your Unity Catalog Volume.
5. Verify the dataset paths in the notebook.
6. Attach compatible compute.
7. Execute the notebook cells sequentially.

### Running Locally

Install PySpark:

```bash
pip install pyspark
```

Verify the installation:

```python
import pyspark

print(pyspark.__version__)
```

**Note:** The notebook contains Databricks-specific functionality such as `display()`, `dbutils.fs`, notebook magic commands and Unity Catalog Volume paths. These features may require modifications to run in a local Spark environment.

---

## Important Learning Concepts

The following concepts are particularly important for understanding how Spark works:

- Distributed data processing.
- DataFrame transformations and actions.
- Lazy evaluation.
- Schema definition and enforcement.
- Data partitioning.
- Spark SQL and DataFrame APIs.
- File formats and storage.
- Unity Catalog Volumes.
- Managed Delta tables.
- Window functions and analytical operations.

---

## Future Learning Goals

This repository will continue to evolve as I explore more advanced Spark and Data Engineering concepts.

- [ ] Spark architecture and execution model.
- [ ] RDD fundamentals.
- [ ] Narrow vs. wide transformations.
- [ ] Transformations vs. actions.
- [ ] `repartition()` and `coalesce()`.
- [ ] Spark caching and persistence.
- [ ] Broadcast joins.
- [ ] Shuffle operations.
- [ ] Spark execution plans using `explain()`.
- [ ] Performance optimization.
- [ ] Delta Lake and transaction logs.
- [ ] Partitioned data storage.
- [ ] Medallion Architecture (Bronze, Silver and Gold).
- [ ] End-to-end ETL pipeline.
- [ ] Data quality and validation.
- [ ] Azure Databricks integration.

---

## Official Documentation

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
- [Delta Lake Documentation](https://docs.delta.io/)

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Author

**Santhosh**

Aspiring Data Engineer | Python | SQL | PySpark | Apache Spark | Databricks

GitHub: [@santhoshgdl](https://github.com/santhoshgdl)

---

<p align="center">
  <strong>Learning by building. Improving through practice.</strong>
</p>

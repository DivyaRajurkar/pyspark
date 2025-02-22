# pyspark
**Mastering PySpark Column Operations**

Apache Spark is a powerful distributed computing framework, and PySpark is its Python API, widely used for big data processing. This blog post explores various column operations in PySpark, covering basic to advanced transformations with detailed explanations and code examples.

---

## **1. Installing and Setting Up PySpark**
Before working with PySpark, install it using:
```python
pip install pyspark
```
Import the required libraries and set up a Spark session:
```python
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, upper, lower, when, count, avg, sum, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("PySparkColumnOperations").getOrCreate()
```

---

## **2. Understanding Row Objects**
### **Basic Row Object Creation**
```python
from pyspark.sql import Row
r1 = Row(1, 'divya')  
print(r1)  # Output: Row(_1=1, _2='divya')
```
Since no field names are provided, PySpark assigns default field names: `_1` and `_2`.

### **Named Row Objects**
Named row objects allow us to define field names explicitly:
```python
r1 = Row(id=1, name="sanket")  
print(r1)  # Output: Row(id=1, name='sanket')
```
Accessing values:
```python
r1['id'], r1['name']  # Output: (1, 'sanket')
```

---

## **3. Creating a DataFrame Using Row Objects**
```python
data = [Row(id=1, name='Alice'), Row(id=2, name='Bob')]
df = spark.createDataFrame(data)
df.show()
```
Output:
```
+---+-----+
| id| name|
+---+-----+
|  1|Alice|
|  2|  Bob|
+---+-----+
```

---

## **4. Column Operations in PySpark**

### **Selecting and Renaming Columns**
Selecting a specific column:
```python
df.select("name").show()
```
Renaming a column:
```python
df = df.withColumnRenamed("name", "full_name")
df.show()
```

### **Adding, Modifying, and Dropping Columns**
Adding a new column:
```python
df = df.withColumn("age", lit(25))
df.show()
```
Modifying an existing column:
```python
df = df.withColumn("full_name", upper(col("full_name")))
df.show()
```
Dropping a column:
```python
df = df.drop("age")
df.show()
```

### **Filtering and Conditional Operations**
Filtering rows:
```python
df.filter(col("id") > 1).show()
```
Using `when` for conditional transformations:
```python
df = df.withColumn("category", when(col("id") == 1, "Admin").otherwise("User"))
df.show()
```

---

## **5. Using AND, OR, and LIKE Conditions**
### **Using AND & OR Conditions**
Filtering with AND condition:
```python
df.filter((col("id") > 1) & (col("full_name") == "BOB")).show()
```
Filtering with OR condition:
```python
df.filter((col("id") > 1) | (col("full_name") == "Alice")).show()
```

### **Using LIKE for Pattern Matching**
Filtering names that start with 'A':
```python
df.filter(col("full_name").like("A%"))
```
Filtering names containing 'li':
```python
df.filter(col("full_name").like("%li%"))
```

---

## **6. Commonly Used PySpark Functions**
### **1️⃣ `lit()` – Literal Value Function**
**Purpose:** Adds a **constant value** to a column.
```python
from pyspark.sql.functions import lit
df = df.withColumn("Country", lit("India"))
df.show()
```

### **2️⃣ `col()` – Column Reference Function**
**Purpose:** Refers to a **column** in a DataFrame by name.
```python
from pyspark.sql.functions import col
df.select(col("name"), col("id")).show()
```

### **3️⃣ `when()` – Conditional Logic Function**
**Purpose:** Implements **IF-ELSE logic** for conditional transformations.
```python
from pyspark.sql.functions import when, col
df = df.withColumn("Performance", when(col("id") > 1, "Average").otherwise("Good"))
df.show()
```

---

## **7. Joining DataFrames in PySpark**
Creating two sample DataFrames:
```python
df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df2 = spark.createDataFrame([(1, "HR"), (2, "Finance")], ["id", "department"])
```
Performing an INNER JOIN:
```python
df_joined = df1.join(df2, on="id", how="inner")
df_joined.show()
```
Performing a LEFT JOIN:
```python
df_joined = df1.join(df2, on="id", how="left")
df_joined.show()
```
Performing a RIGHT JOIN:
```python
df_joined = df1.join(df2, on="id", how="right")
df_joined.show()
```

---

## **8. User-Defined Functions (UDFs)**
Custom transformations using UDFs:
```python
def custom_greeting(name):
    return f"Hello, {name}!"

greet_udf = udf(custom_greeting, StringType())
df = df.withColumn("greeting", greet_udf(col("full_name")))
df.show()
```

---

## **9. Handling Null Values**
Filling null values:
```python
df.fillna({"full_name": "Unknown"}).show()
```
Dropping rows with null values:
```python
df.na.drop().show()
```

---

## **Conclusion**
PySpark column operations are essential for efficient data processing. Whether selecting, renaming, filtering, applying UDFs, handling null values, or performing joins, these techniques enhance data analysis workflows.

**Have questions or favorite PySpark techniques? Share them in the comments!**

---

**Tags:** #PySpark #BigData #DataEngineering #SparkSQL #DataProcessing


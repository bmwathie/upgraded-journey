# **Introduction to Polars**
 One of the fastest Dataframe library at the moment.  
![Polars](https://github.com/bmwathie/Examen_Python-M1_IA_Nov_2022/blob/main/img/t%C3%A9l%C3%A9chargement.png?raw=true "Polars") 
# **Contents**
- **Introduction**
- **Concept**
- **Installing Polars**
- **Creating a Polars DataFrame**
- **Importants attributes and function to know**
- **Potential alternatives**
- **Concrete use cases**
- **Data processing with Polars**
    * **1. Dataset Inspection** 
    * **2. Removing nulls**
    * **3. Some analyses**
    * **4. Filtration**
    * **5. Ploting with polars**
    * **6. Time to go Lazy**
    * **Output**
- **Out of Core (NEW)**
- **Conclusion**
- **List of consulted resources**
## **Introduction**
**Polars** library is a ast in-memory tabular data processing library designed to be used in conjunction with Python. It was developed to offer high performance and advanced features for processing large-scale data. In this presentation, we will review the fundamental concepts of Polars, as well as practical examples, potential alternatives, and use cases.
## **Concept**
Polars is a Python library for in-memory tabular data processing that provides advanced features for handling large-scale data. It is designed to be a performant and efficient alternative to traditional data processing libraries like Pandas. Polars leverages Rust, a high-performance systems programming language, to provide the necessary speed and performance for large datasets.

Polars uses a DataFrame as its core data structure, similar to Pandas. However, unlike Pandas, Polars is designed to handle larger datasets efficiently by taking advantage of Rust's memory management and parallelism features. Polars supports a wide range of data types and operations, including filtering, grouping, aggregating, and joining data.
## **Installing Polars**
To install Polars, you can simply use the `pip` command :  
`pip install polars`  
Or, use the `conda` command :  
`conda install polars`  
## **Creating a Polars DataFrame**
The best way to learn a new library is to get your hands dirty. Let's get started by importing the ***polars*** module and creating a Polars DataFrame:  
```python
import polars as pl
df = pl.DataFrame(
    {
        'Name': ['mbaye','moustapha','cheikh','patrick','aliou',
                    'charles','jean','jhon','anna','christine'],
        'email': ['mbaye@gmail.com','moustapha@gmail.com','cheikh@gmail.com', 'patrick@gmail.com','aliou@gmail.com',     
                  'charles@gmail.com','jean@gmail.com','jhon@gmail.com','anna@gmail.com','christine@gmail.com'],
        'salary en FCFA': [350000, 250000, 150000, 1300000, 175000, 500000, 900000, 850000, 175000, 235000]
    }
)
df  
```
![output](https://github.com/bmwathie/Examen_Python-M1_IA_Nov_2022/blob/main/img/Capture%20d%E2%80%99%C3%A9cran%20du%202023-03-06%2016-37-06.png?raw=true "output")  
Polars expects the column header names to be of string type. Consider the following example:  
```python
df2 = pl.DataFrame(
    {
        0 : [1,2,3],
        1 : [80,170,130],
    }
)
```
The above code snippet won't work as the keys in the dictionary are of type integer (0 and 1). To make it work, you need to make sure the keys are of string type (“0” and “1”):  
```python
df2 = pl.DataFrame(
    {
        "0" : [1,2,3],
        "1" : [80,170,130],
    }
)
```  
Besides displaying the header name for each column, Polars also displays the data type of each column. 
## **Important attributes and function to know**  
- **.dtypes** : display the data type
- **.columns** : to get the column names  
- **.rows()** : to get the content of the DataFrame as a list of tuples 
- **.row()** : displays a specific row (the specification is done between parentheses, ex: df.row(0)). The result is a tuple.
- **.select()** : displays data from the specified column  
- **.filter()** : to select multiple rows  
- **.groupby(), .agg(), .pivot_table()** : functions allow to aggregate data using various aggregation functions such as sum, average, maximum, minimum, etc.
- **.Join(), .merge()** : functions allow to join data from multiple dataframes.  

Note that this list is not exhaustive, for more details you can consult the complete documentation : https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html  
## **Potential alternatives**  
These are some tools that share similar functionality to what polars does.

- Pandas:  
    * A very versatile tool for small data. Read 10 things I hate about pandas written by the author himself. Polars has solved all those 10 things. Polars is a versatile tool for small and large data with a more predictable API, less ambiguous and stricter API.
- Pandas the API :  
    * The API of pandas was designed for in memory data. This makes it a poor fit for performant analysis on large data (read anything that does not fit into RAM). Any tool that tries to distribute that API will likely have a suboptimal query plan compared to plans that follow from a declarative API like SQL or polars' API.
- Dask :   
    * Parallelizes existing single-threaded libraries like NumPy and Pandas. As a consumer of those libraries Dask therefore has less control over low level performance and semantics. Those libraries are treated like a black box. On a single machine the parallelization effort can also be seriously stalled by pandas strings. Pandas strings, by default, are stored as python objects in numpy arrays meaning that any operation on them is GIL bound and therefore single threaded. This can be circumvented by multi-processing but has a non-trivial cost.
- Modin :  
    * Similar to Dask
- Vaex :

    * Vaexs method of out-of-core analysis is memory mapping files. This works until it doesn't. For instance parquet or csv files first need to be read and converted to a file format that can be memory mapped. Another downside is that the OS determines when pages will be swapped. Operations that need a full data shuffle, such as sorts cannot benefit from memory mapping. At the moment of writing vaex relies on pyarrow for sorts, meaning that the data must fit into memory.
    * Polars' out of core processing is not based on memory mapping, but on streaming data in batches (and spilling to disk if needed), we control which data must be hold in memory, not the OS, meaning that we don't have unexpected IO stalls.
- DuckDB :  
    * Polars and DuckDB have many similarities. DuckDB is focussed on providing an in-process OLAP Sqlite alternative, polars is focussed on providing a scalable DataFrame interface to many languages. Those different front-ends lead to different optimization strategies and different algorithm prioritization. The interop between both is zero-copy. See more: https://duckdb.org/docs/guides/python/polars
- Spark :  
    * Spark is designed for distributed workloads and uses the JVM. The setup for spark is complicated and the startup-time is slow. Polars has much better performance characteristics on a single machine. The API's are somewhat similar.
- CuDF  
    * GPU's are fast, but not readily available and expensive in production. The amount of memory available on GPU often is a fraction of available RAM. Next to that Polars is close in performance to CuDF and on some operations even faster. CuDF also doesn't optimize your query, so it is likely that on ETL jobs polars will be faster because it can elide unneeded work and materialization's.
- Any :  
    * Polars is written in Rust. This gives it strong safety, performance and concurrency guarantees. Polars is written in a modular manner. Parts of polars can be used in other query program and can be added as a library.
## **Concrete use cases**
The polars library in Python is a high-performance data manipulation library that can be used for various tasks, such as:

- Data exploration: polars can be used to explore large and complex data, performing operations such as selecting columns, filtering rows, sorting, grouping, merging data, and calculating statistics.

- Real-time data processing: polars can be used to process real-time data streams, such as sensor data, monitoring data, and trading data.

- Machine learning: polars can be used to prepare and clean data for use in machine learning models.

- Financial analysis: polars is particularly well-suited for financial analysis due to its ability to handle large and complex data and perform operations such as calculating returns, grouping data by date, and calculating financial statistics.

- Geospatial analysis: polars can be used to analyze geospatial data, such as GPS coordinates, by performing operations such as coordinate transformation, merging geospatial data, and calculating distances between points.  

But we will use it to process data from a downloaded dataset on Kaggle.
## **Data processing with Polars**
What's better than practice to learn?  
Polars already offers many functionalities that we are already familiar if you have worked with Pandas before. We can find an overview, including examples (for most), in the reference guide : https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html  
Link to the dataset used : https://www.kaggle.com/datasets/zynicide/wine-reviews?resource=download   
```python
# importing libraries
import polars as pl
import matplotlib.pyplot as plt
# Downloading data into a DataFrame
>>> df = pl.read_csv('/content/drive/MyDrive/winemag-data_first150k.csv')
# Displaying type of df
>>> print(type(df))
<class 'polars.internals.dataframe.frame.DataFrame'>
```
### **1. Dataset Inspection**  
```python
# Displaying dimension of df
>>> df.shape
(150930, 11)
# Displaying all columns of df
>>> df.columns
['', 'country', 'description', 'designation', 'points', 'price', 'province', 'region_1', 'region_2', 'variety', 'winery']
# Displaying type of columns in df
>>> df.dtypes
[Int64, Utf8, Utf8, Utf8, Int64, Float64, Utf8, Utf8, Utf8, Utf8, Utf8]
# Below we use sample() to get random rows from the dataset to get a feeling of the data that is available. Polars also offers common functions like head() and tail()
>>> df.sample(n=5)
shape: (5, 11)
┌────────┬─────────┬───────────┬───────────┬─────┬────────────┬────────────┬────────────┬───────────┐
│        ┆ country ┆ descripti ┆ designati ┆ ... ┆ region_1   ┆ region_2   ┆ variety    ┆ winery    │
│ ---    ┆ ---     ┆ on        ┆ on        ┆     ┆ ---        ┆ ---        ┆ ---        ┆ ---       │
│ i64    ┆ str     ┆ ---       ┆ ---       ┆     ┆ str        ┆ str        ┆ str        ┆ str       │
│        ┆         ┆ str       ┆ str       ┆     ┆            ┆            ┆            ┆           │
╞════════╪═════════╪═══════════╪═══════════╪═════╪════════════╪════════════╪════════════╪═══════════╡
│ 24988  ┆ Spain   ┆ Foresty   ┆ Viña      ┆ ... ┆ Rioja      ┆ null       ┆ Tempranill ┆ Bodegas   │
│        ┆         ┆ aromas    ┆ Albina    ┆     ┆            ┆            ┆ o Blend    ┆ Riojanas  │
│        ┆         ┆ blend     ┆ Reserva   ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ with      ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ cola, ... ┆           ┆     ┆            ┆            ┆            ┆           │
│ 70289  ┆ France  ┆ Intended  ┆ Renaissan ┆ ... ┆ Cornas     ┆ null       ┆ Syrah      ┆ Auguste   │
│        ┆         ┆ to offer  ┆ ce        ┆     ┆            ┆            ┆            ┆ et Pierre │
│        ┆         ┆ a taste   ┆           ┆     ┆            ┆            ┆            ┆ -Marie    │
│        ┆         ┆ of Cor... ┆           ┆     ┆            ┆            ┆            ┆ Clape     │
│ 13810  ┆ France  ┆ This is a ┆ Nouveau   ┆ ... ┆ Beaujolais ┆ null       ┆ Gamay      ┆ Henry     │
│        ┆         ┆ festive   ┆           ┆     ┆            ┆            ┆            ┆ Fessy     │
│        ┆         ┆ wine,     ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ with      ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ sof...    ┆           ┆     ┆            ┆            ┆            ┆           │
│ 71416  ┆ Spain   ┆ Plenty of ┆ Prios     ┆ ... ┆ Ribera del ┆ null       ┆ Tempranill ┆ Bodegas   │
│        ┆         ┆ intense   ┆ Maximus   ┆     ┆ Duero      ┆            ┆ o          ┆ de los    │
│        ┆         ┆ new oak   ┆ Crianza   ┆     ┆            ┆            ┆            ┆ Rios      │
│        ┆         ┆ sets t... ┆           ┆     ┆            ┆            ┆            ┆ Prieto    │
│ 122762 ┆ US      ┆ Simple,   ┆ null      ┆ ... ┆ Paso       ┆ Central    ┆ Chardonnay ┆ Eagle     │
│        ┆         ┆ with      ┆           ┆     ┆ Robles     ┆ Coast      ┆            ┆ Castle    │
│        ┆         ┆ fruit pun ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ ch-sweet  ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ p...      ┆           ┆     ┆            ┆            ┆            ┆           │
└────────┴─────────┴───────────┴───────────┴─────┴────────────┴────────────┴────────────┴───────────┘
```
The dataset has a lot to offer. With 11 variables and over 150k rows, there is a lot of data to analyse. We see a couple of variables that are interesting to look into, like `price`, `country`, `points`.  
### **2. Removing nulls**   
Before we continue we want to have a closer look at if there are any nulls in the dataset.
```python
# Printing doesn't show all columns
>>> df.null_count()
shape: (1, 11)
┌─────┬─────────┬─────────────┬─────────────┬─────┬──────────┬──────────┬─────────┬────────┐
│     ┆ country ┆ description ┆ designation ┆ ... ┆ region_1 ┆ region_2 ┆ variety ┆ winery │
│ --- ┆ ---     ┆ ---         ┆ ---         ┆     ┆ ---      ┆ ---      ┆ ---     ┆ ---    │
│ u32 ┆ u32     ┆ u32         ┆ u32         ┆     ┆ u32      ┆ u32      ┆ u32     ┆ u32    │
╞═════╪═════════╪═════════════╪═════════════╪═════╪══════════╪══════════╪═════════╪════════╡
│ 0   ┆ 5       ┆ 0           ┆ 45735       ┆ ... ┆ 25060    ┆ 89977    ┆ 0       ┆ 0      │
└─────┴─────────┴─────────────┴─────────────┴─────┴──────────┴──────────┴─────────┴────────┘
# let's overwrite df keeping the columns that we deem relevant
>>> df = df[['country', 'points', 'price']]
>>> df.null_count()
shape: (1, 3)
┌─────────┬────────┬───────┐
│ country ┆ points ┆ price │
│ ---     ┆ ---    ┆ ---   │
│ u32     ┆ u32    ┆ u32   │
╞═════════╪════════╪═══════╡
│ 5       ┆ 0      ┆ 13695 │
└─────────┴────────┴───────┘
# removal of missing values in country and price
# strategy="mean" for replace the zero values by the mean of price
>>> df = df.with_column(pl.col("price").fill_null(strategy="mean").alias('price'))
# strategy="forward" to replace the missing value with the value of the next line  
>>> df = df.with_column(pl.col("country").fill_null(strategy="forward").alias('country'))
>>> df.null_count()
shape: (1, 3)
┌─────────┬────────┬───────┐
│ country ┆ points ┆ price │
│ ---     ┆ ---    ┆ ---   │
│ u32     ┆ u32    ┆ u32   │
╞═════════╪════════╪═══════╡
│ 0       ┆ 0      ┆ 0     │
└─────────┴────────┴───────┘
```
### **3. Some analyses**
The next step is to dive in a little deeper and have a closer look at the dataset with some more complex functions.

The goal that we want to achieve in the following part is to have a closer look at the countries and how they compare in terms of price and points.
```python
# Analyses of wine prices
# median, min, max, mean of price display in df format
>>> df.select([pl.median("price").alias("median price"),pl.min("price").alias("min price"),pl.max("price").alias("max price"),pl.mean("price").alias("mean price")])
shape: (1, 4)
┌──────────────┬───────────┬───────────┬────────────┐
│ median price ┆ min price ┆ max price ┆ mean price │
│ ---          ┆ ---       ┆ ---       ┆ ---        │
│ f64          ┆ f64       ┆ f64       ┆ f64        │
╞══════════════╪═══════════╪═══════════╪════════════╡
│ 26.0         ┆ 4.0       ┆ 2300.0    ┆ 33.131482  │
└──────────────┴───────────┴───────────┴────────────┘
# Analyses of wine points
# median, min, max, mean of points display in df format
>>> df.select([pl.median("points").alias("median points"),pl.min("points").alias("min points"),pl.max("points").alias("max points"),pl.mean("points").alias("mean points")])
shape: (1, 4)
┌───────────────┬────────────┬────────────┬─────────────┐
│ median points ┆ min points ┆ max points ┆ mean points │
│ ---           ┆ ---        ┆ ---        ┆ ---         │
│ f64           ┆ i64        ┆ i64        ┆ f64         │
╞═══════════════╪════════════╪════════════╪═════════════╡
│ 88.0          ┆ 80         ┆ 100        ┆ 87.888418   │
└───────────────┴────────────┴────────────┴─────────────┘
```
Or we can just use the describe() function for quick statistics about our Dataframe.
```python
>>> df.describe()
# in column country, mean and std and median are null because the column counstry contain values type str
shape: (7, 4)
┌────────────┬─────────┬───────────┬───────────┐
│ describe   ┆ country ┆ points    ┆ price     │
│ ---        ┆ ---     ┆ ---       ┆ ---       │
│ str        ┆ str     ┆ f64       ┆ f64       │
╞════════════╪═════════╪═══════════╪═══════════╡
│ count      ┆ 150930  ┆ 150930.0  ┆ 150930.0  │
│ null_count ┆ 0       ┆ 0.0       ┆ 0.0       │
│ mean       ┆ null    ┆ 87.888418 ┆ 33.131482 │
│ std        ┆ null    ┆ 3.222392  ┆ 34.635438 │
│ min        ┆ Albania ┆ 80.0      ┆ 4.0       │
│ max        ┆ Uruguay ┆ 100.0     ┆ 2300.0    │
│ median     ┆ null    ┆ 88.0      ┆ 26.0      │
└────────────┴─────────┴───────────┴───────────┘
```
### **4. Filtration**  
```python
# list the contain of country only
>>> df['country'].unique().to_list()
['Morocco', 'Slovenia', 'Macedonia', 'France', 'China', 'Romania', 'Cyprus', 'Argentina', 'Albania', 'Georgia', 'Austria', 'Japan', 'Uruguay', 'Australia', 'New Zealand', 'Lithuania', 'Canada', 'Spain', 'Greece', 'Slovakia', 'Moldova', 'India', 'Montenegro', 'Bosnia and Herzegovina', 'Israel', 'Ukraine', 'Egypt', 'Italy', 'Chile', 'Turkey', 'Brazil', 'Germany', 'Mexico', 'Switzerland', 'South Africa', 'Czech Republic', 'Bulgaria', 'England', 'Lebanon', 'US-France', 'Croatia', 'Portugal', 'US', 'Tunisia', 'Hungary', 'Serbia', 'South Korea', 'Luxembourg'] # rom our earlier output, we find outliers "US-France" and "Bosnia and Herzegovina", make a filter to list them
>>> df.filter((pl.col('country') == 'US-France') | (pl.col('country').is_null()))
# only country "US-France"
shape: (1, 3)
┌───────────┬────────┬───────┐
│ country   ┆ points ┆ price │
│ ---       ┆ ---    ┆ ---   │
│ str       ┆ i64    ┆ f64   │
╞═══════════╪════════╪═══════╡
│ US-France ┆ 88     ┆ 50.0  │
└───────────┴────────┴───────┘
>>> df.filter((pl.col('country') == 'Bosnia and Herzegovina') | (pl.col('country').is_null()))
# 4 country "Bosnia and Herzegovina"
shape: (4, 3)
┌────────────────────────┬────────┬───────┐
│ country                ┆ points ┆ price │
│ ---                    ┆ ---    ┆ ---   │
│ str                    ┆ i64    ┆ f64   │
╞════════════════════════╪════════╪═══════╡
│ Bosnia and Herzegovina ┆ 88     ┆ 12.0  │
│ Bosnia and Herzegovina ┆ 83     ┆ 13.0  │
│ Bosnia and Herzegovina ┆ 85     ┆ 13.0  │
│ Bosnia and Herzegovina ┆ 83     ┆ 13.0  │
└────────────────────────┴────────┴───────┘
# Filter out rows in `df` where the `country` column is not null and not equal to 'US-France'
>>> df = df.filter((pl.col('country').is_not_null()) & (pl.col('country') != 'US-France'))
# Filter out rows in `df` where the `country` column is not null and not equal to 'Bosnia and Herzegovina'
>>> df = df.filter((pl.col('country').is_not_null()) & (pl.col('country') != 'Bosnia and Herzegovina'))
```
Time to look into the countries that produces the best wine according to the points and has the highest price for a bottle.
```python
# Group the DataFrame `df` by country and aggregate the mean of the `points` column for each country
    # Sort the resulting DataFrame by the `points_mean` column in descending order
>>> df.groupby('country').agg(pl.col('points').mean().alias('points_mean')).sort(by='points_mean', reverse=True)
shape: (47, 2)
┌─────────────┬─────────────┐
│ country     ┆ points_mean │
│ ---         ┆ ---         │
│ str         ┆ f64         │
╞═════════════╪═════════════╡
│ England     ┆ 92.888889   │
│ Austria     ┆ 89.276742   │
│ France      ┆ 88.92587    │
│ Germany     ┆ 88.626427   │
│ ...         ┆ ...         │
│ Brazil      ┆ 83.24       │
│ China       ┆ 82.0        │
│ Montenegro  ┆ 82.0        │
│ South Korea ┆ 81.5        │
└─────────────┴─────────────┘
```
England is leading the list for the best wines. Wonder how they think about that on the other side of the Canal in France.
```python
# Group data in the DataFrame `df` by the `country` column
    # Aggregate the maximum value of the `price` column for each group
        # Alias the resulting column as `price_max`
            # Sort the resulting DataFrame by the `price_max` column in descending order
>>> df.groupby('country').agg(pl.col('price').max().alias('price_max')).sort(by='price_max', reverse=True)
shape: (47, 2)
┌────────────────────────┬───────────┐
│ country                ┆ price_max │
│ ---                    ┆ ---       │
│ str                    ┆ f64       │
╞════════════════════════╪═══════════╡
│ France                 ┆ 2300.0    │
│ US                     ┆ 2013.0    │
│ Austria                ┆ 1100.0    │
│ Portugal               ┆ 980.0     │
│ ...                    ┆ ...       │
│ Ukraine                ┆ 13.0      │
│ Bosnia and Herzegovina ┆ 13.0      │
│ Montenegro             ┆ 10.0      │
│ Lithuania              ┆ 10.0      │
└────────────────────────┴───────────┘
```
### **5. Ploting with polars**
To get a better insight into the differences it always helps to have some nice plots. Where Pandas has a plotting functionality build in, we have to rely on our Matplotlib skills for Polars. We focus on the top 15 countries.
```python
# Group the data in the `df` DataFrame by country, compute the mean of the `points` column, and rename the result as `points_mean`
    # Sort the resulting DataFrame by the `points_mean` column in descending order
        # Limit the number of rows in the resulting DataFrame to 15
>>> best_15_countries = df.groupby('country').agg(pl.col('points').mean().alias('points_mean')).sort(by='points_mean', reverse=True).limit(15)
>>> best_15_countries
shape: (15, 2)
┌───────────┬─────────────┐
│ country   ┆ points_mean │
│ ---       ┆ ---         │
│ str       ┆ f64         │
╞═══════════╪═════════════╡
│ England   ┆ 92.888889   │
│ Austria   ┆ 89.276742   │
│ France    ┆ 88.92587    │
│ Germany   ┆ 88.626427   │
│ ...       ┆ ...         │
│ Australia ┆ 87.892475   │
│ US        ┆ 87.818824   │
│ Serbia    ┆ 87.714286   │
│ India     ┆ 87.625      │
└───────────┴─────────────┘
# join df and best_15_countries to make a df_top15
>>> df_top15 = best_15_countries.join(df, on='country', how='left')
>>> df_top15
shape: (123149, 4)
┌─────────┬─────────────┬────────┬───────┐
│ country ┆ points_mean ┆ points ┆ price │
│ ---     ┆ ---         ┆ ---    ┆ ---   │
│ str     ┆ f64         ┆ i64    ┆ f64   │
╞═════════╪═════════════╪════════╪═══════╡
│ England ┆ 92.888889   ┆ 94     ┆ 45.0  │
│ England ┆ 92.888889   ┆ 94     ┆ 38.0  │
│ England ┆ 92.888889   ┆ 94     ┆ 49.0  │
│ England ┆ 92.888889   ┆ 94     ┆ 44.0  │
│ ...     ┆ ...         ┆ ...    ┆ ...   │
│ India   ┆ 87.625      ┆ 90     ┆ 10.0  │
│ India   ┆ 87.625      ┆ 87     ┆ 12.0  │
│ India   ┆ 87.625      ┆ 82     ┆ 20.0  │
│ India   ┆ 87.625      ┆ 82     ┆ 20.0  │
└─────────┴─────────────┴────────┴───────┘
# figure sizing
>>> fig, ax = plt.subplots(figsize=(15, 5))
# Iteration over country-partitioned data groups in the `df_top15` DataFrame
    # Retrieve the country name from the first row of `country_df`
        # Plot a box plot for the `points` column of `country_df`
>>> for i, country_df in enumerate(df_top15.partition_by(groups="country")):
...     country_name = country_df.select("country")[0, 0]
...     ax.boxplot(country_df.select('points'), labels=[country_name], positions=[i])
# Customize the x-axis
>>> plt.xticks(rotation=90)
>>> plt.xlabel('Countries')
>>> plt.ylabel('Average points')
>>> plt.show()
```
![fig_courbe](https://github.com/bmwathie/Examen_Python-M1_IA_Nov_2022/blob/main/img/tracer.png)
### **6. Time to go Lazy**
The lazy API offers a way to optimise your queries, similar to Spark. The major benefit over spark is that we don’t have to set up our environment and can therefore continue working from our notebook.  
```python
# Dowload dataset to lazy_df
>>> lazy_df = pl.scan_csv('/home/sulamu/BMW/Master IA/Python/Exam_Python/Examen_Python_M1-IA_Nov-2022/Examen_Python-M1_IA_Nov_2022/Dataset used/archive/winemag-data_first150k.csv')
>>> lazy_df
<polars.LazyFrame object at 0x7F80047471C0>
```
![lazyframe1](https://github.com/bmwathie/Examen_Python-M1_IA_Nov_2022/blob/main/img/lazyframe1.png)  
Printing the type returns ‘polars.lazy.LazyFrame’ indicating the data is available to us. On to the Groupby `country` and find the average `points` to compare with the eager API that we used earlier.

Similar to the filters that we did with the eager API we are going to filter the unknown and ‘US-France’ values in the `country` variable first.
```python
>>> lazy_df.filter((pl.col('country').is_not_null()) & (pl.col('country') != 'US-France'))
<polars.LazyFrame object at 0x7F80047472B0>
```
![lazyframe](https://github.com/bmwathie/Examen_Python-M1_IA_Nov_2022/blob/main/img/lazyframe.png)  
We can see that the query is almost the same, however this query only returns a query plan.

As we can see nothing happens right away. From the documentation: ‘This is due to the lazyness, nothing will happen until specifically requested. This allows Polars to see the whole context of a query and optimize just in time for execution.’

To actually see the results we can do two things: `collect()` and `fetch()`. The difference is that fetch takes the first 500 rows and then runs the query, whereas `collect` runs the query over all the results. Below we can see the differences for our case.
```python
# Filter data in the `lazy_df` DataFrame
    # Retrieve results as a set of rows
>>> lazy_df.filter((pl.col('country').is_not_null()) & (pl.col('country') != 'US-France')).fetch()
shape: (500, 11)
┌─────┬─────────┬───────────┬───────────┬─────┬─────────────┬─────────────┬────────────┬────────────┐
│     ┆ country ┆ descripti ┆ designati ┆ ... ┆ region_1    ┆ region_2    ┆ variety    ┆ winery     │
│ --- ┆ ---     ┆ on        ┆ on        ┆     ┆ ---         ┆ ---         ┆ ---        ┆ ---        │
│ i64 ┆ str     ┆ ---       ┆ ---       ┆     ┆ str         ┆ str         ┆ str        ┆ str        │
│     ┆         ┆ str       ┆ str       ┆     ┆             ┆             ┆            ┆            │
╞═════╪═════════╪═══════════╪═══════════╪═════╪═════════════╪═════════════╪════════════╪════════════╡
│ 0   ┆ US      ┆ This trem ┆ Martha's  ┆ ... ┆ Napa Valley ┆ Napa        ┆ Cabernet   ┆ Heitz      │
│     ┆         ┆ endous    ┆ Vineyard  ┆     ┆             ┆             ┆ Sauvignon  ┆            │
│     ┆         ┆ 100%      ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ varietal  ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ wi...     ┆           ┆     ┆             ┆             ┆            ┆            │
│ 1   ┆ Spain   ┆ Ripe      ┆ Carodorum ┆ ... ┆ Toro        ┆ null        ┆ Tinta de   ┆ Bodega     │
│     ┆         ┆ aromas of ┆ Selección ┆     ┆             ┆             ┆ Toro       ┆ Carmen     │
│     ┆         ┆ fig, blac ┆ Especial  ┆     ┆             ┆             ┆            ┆ Rodríguez  │
│     ┆         ┆ kberry    ┆ Res...    ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ a...      ┆           ┆     ┆             ┆             ┆            ┆            │
│ 2   ┆ US      ┆ Mac       ┆ Special   ┆ ... ┆ Knights     ┆ Sonoma      ┆ Sauvignon  ┆ Macauley   │
│     ┆         ┆ Watson    ┆ Selected  ┆     ┆ Valley      ┆             ┆ Blanc      ┆            │
│     ┆         ┆ honors    ┆ Late      ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ the       ┆ Harvest   ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ memory of ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ ...       ┆           ┆     ┆             ┆             ┆            ┆            │
│ 3   ┆ US      ┆ This      ┆ Reserve   ┆ ... ┆ Willamette  ┆ Willamette  ┆ Pinot Noir ┆ Ponzi      │
│     ┆         ┆ spent 20  ┆           ┆     ┆ Valley      ┆ Valley      ┆            ┆            │
│     ┆         ┆ months in ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ 30% new   ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ ...       ┆           ┆     ┆             ┆             ┆            ┆            │
│ ... ┆ ...     ┆ ...       ┆ ...       ┆ ... ┆ ...         ┆ ...         ┆ ...        ┆ ...        │
│ 496 ┆ US      ┆ Boysenber ┆ Thompson  ┆ ... ┆ Santa       ┆ Central     ┆ Syrah      ┆ Rideau     │
│     ┆         ┆ ry, dried ┆ Vineyard  ┆     ┆ Barbara     ┆ Coast       ┆            ┆            │
│     ┆         ┆ violets   ┆           ┆     ┆ County      ┆             ┆            ┆            │
│     ┆         ┆ and b...  ┆           ┆     ┆             ┆             ┆            ┆            │
│ 497 ┆ Italy   ┆ This      ┆ Daniello  ┆ ... ┆ Toscana     ┆ null        ┆ Red Blend  ┆ Tenuta di  │
│     ┆         ┆ blend of  ┆           ┆     ┆             ┆             ┆            ┆ Trecciano  │
│     ┆         ┆ 80%       ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ Cabernet  ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ Sauvi...  ┆           ┆     ┆             ┆             ┆            ┆            │
│ 498 ┆ Italy   ┆ This      ┆ Il        ┆ ... ┆ Toscana     ┆ null        ┆ Cabernet   ┆ Terre del  │
│     ┆         ┆ brooding  ┆ Tarabuso  ┆     ┆             ┆             ┆ Sauvignon  ┆ Marchesato │
│     ┆         ┆ wine      ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ opens     ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ with      ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ ar...     ┆           ┆     ┆             ┆             ┆            ┆            │
│ 499 ┆ Spain   ┆ Earthy    ┆ null      ┆ ... ┆ Priorat     ┆ null        ┆ Red Blend  ┆ Vega Escal │
│     ┆         ┆ berry and ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ licorice  ┆           ┆     ┆             ┆             ┆            ┆            │
│     ┆         ┆ aromas... ┆           ┆     ┆             ┆             ┆            ┆            │
└─────┴─────────┴───────────┴───────────┴─────┴─────────────┴─────────────┴────────────┴────────────┘
# Filter data in the `lazy_df` DataFrame
    # Retrieve results as a local list 
>>> lazy_df.filter((pl.col('country').is_not_null()) & (pl.col('country') != 'US-France')).collect()
shape: (150924, 11)
┌────────┬─────────┬───────────┬───────────┬─────┬────────────┬────────────┬────────────┬───────────┐
│        ┆ country ┆ descripti ┆ designati ┆ ... ┆ region_1   ┆ region_2   ┆ variety    ┆ winery    │
│ ---    ┆ ---     ┆ on        ┆ on        ┆     ┆ ---        ┆ ---        ┆ ---        ┆ ---       │
│ i64    ┆ str     ┆ ---       ┆ ---       ┆     ┆ str        ┆ str        ┆ str        ┆ str       │
│        ┆         ┆ str       ┆ str       ┆     ┆            ┆            ┆            ┆           │
╞════════╪═════════╪═══════════╪═══════════╪═════╪════════════╪════════════╪════════════╪═══════════╡
│ 0      ┆ US      ┆ This trem ┆ Martha's  ┆ ... ┆ Napa       ┆ Napa       ┆ Cabernet   ┆ Heitz     │
│        ┆         ┆ endous    ┆ Vineyard  ┆     ┆ Valley     ┆            ┆ Sauvignon  ┆           │
│        ┆         ┆ 100%      ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ varietal  ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ wi...     ┆           ┆     ┆            ┆            ┆            ┆           │
│ 1      ┆ Spain   ┆ Ripe      ┆ Carodorum ┆ ... ┆ Toro       ┆ null       ┆ Tinta de   ┆ Bodega    │
│        ┆         ┆ aromas of ┆ Selección ┆     ┆            ┆            ┆ Toro       ┆ Carmen    │
│        ┆         ┆ fig, blac ┆ Especial  ┆     ┆            ┆            ┆            ┆ Rodríguez │
│        ┆         ┆ kberry    ┆ Res...    ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ a...      ┆           ┆     ┆            ┆            ┆            ┆           │
│ 2      ┆ US      ┆ Mac       ┆ Special   ┆ ... ┆ Knights    ┆ Sonoma     ┆ Sauvignon  ┆ Macauley  │
│        ┆         ┆ Watson    ┆ Selected  ┆     ┆ Valley     ┆            ┆ Blanc      ┆           │
│        ┆         ┆ honors    ┆ Late      ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ the       ┆ Harvest   ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ memory of ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ ...       ┆           ┆     ┆            ┆            ┆            ┆           │
│ 3      ┆ US      ┆ This      ┆ Reserve   ┆ ... ┆ Willamette ┆ Willamette ┆ Pinot Noir ┆ Ponzi     │
│        ┆         ┆ spent 20  ┆           ┆     ┆ Valley     ┆ Valley     ┆            ┆           │
│        ┆         ┆ months in ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ 30% new   ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ ...       ┆           ┆     ┆            ┆            ┆            ┆           │
│ ...    ┆ ...     ┆ ...       ┆ ...       ┆ ... ┆ ...        ┆ ...        ┆ ...        ┆ ...       │
│ 150926 ┆ France  ┆ Offers an ┆ Cuvée     ┆ ... ┆ Champagne  ┆ null       ┆ Champagne  ┆ H.Germain │
│        ┆         ┆ intriguin ┆ Prestige  ┆     ┆            ┆            ┆ Blend      ┆           │
│        ┆         ┆ g nose    ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ with g... ┆           ┆     ┆            ┆            ┆            ┆           │
│ 150927 ┆ Italy   ┆ This      ┆ Terre di  ┆ ... ┆ Fiano di   ┆ null       ┆ White      ┆ Terredora │
│        ┆         ┆ classic   ┆ Dora      ┆     ┆ Avellino   ┆            ┆ Blend      ┆           │
│        ┆         ┆ example   ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ comes     ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ from ...  ┆           ┆     ┆            ┆            ┆            ┆           │
│ 150928 ┆ France  ┆ A perfect ┆ Grand     ┆ ... ┆ Champagne  ┆ null       ┆ Champagne  ┆ Gosset    │
│        ┆         ┆ salmon    ┆ Brut Rosé ┆     ┆            ┆            ┆ Blend      ┆           │
│        ┆         ┆ shade,    ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ with      ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ sce...    ┆           ┆     ┆            ┆            ┆            ┆           │
│ 150929 ┆ Italy   ┆ More      ┆ null      ┆ ... ┆ Alto Adige ┆ null       ┆ Pinot      ┆ Alois     │
│        ┆         ┆ Pinot     ┆           ┆     ┆            ┆            ┆ Grigio     ┆ Lageder   │
│        ┆         ┆ Grigios   ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ should    ┆           ┆     ┆            ┆            ┆            ┆           │
│        ┆         ┆ taste ... ┆           ┆     ┆            ┆            ┆            ┆           │
└────────┴─────────┴───────────┴───────────┴─────┴────────────┴────────────┴────────────┴───────────┘
```
We can see from the shapes that `fetch` catches 500 rows and `collect` retrieves all the rows.
```python
# Aggregate data in the `lazy_df` DataFrame
    # Sort results in descending order by the `points_mean` column
        # Retrieve results as a local list
>>> lazy_df.groupby('country').agg(pl.col('points').mean().alias('points_mean')).sort(by='points_mean', reverse=True).collect()
shape: (49, 2)
┌─────────────┬─────────────┐
│ country     ┆ points_mean │
│ ---         ┆ ---         │
│ str         ┆ f64         │
╞═════════════╪═════════════╡
│ England     ┆ 92.888889   │
│ Austria     ┆ 89.276742   │
│ France      ┆ 88.92587    │
│ Germany     ┆ 88.626427   │
│ ...         ┆ ...         │
│ Brazil      ┆ 83.24       │
│ China       ┆ 82.0        │
│ Montenegro  ┆ 82.0        │
│ South Korea ┆ 81.5        │
└─────────────┴─────────────┘
```
## **Out of Core (NEW)**  
What if you dataset doesn’t fit in memory? This example is rather small, but in this day and age it is not unlikely that you are working on datsets that don’t fit in memory any more. Polars offers a very easy way to work with that.

Pretend that our dataset is not ~50MB, but 50GB. What can we do to, for example, Groupby country and do some calculations:
```python
# Aggregate data in the `lazy_df` DataFrame
    # Sort results in descending order by the `points_mean` column
        # Retrieve results as a local list
>>> lazy_df.groupby('country').agg(pl.col('points').mean().alias('points_mean')).sort(by='points_mean', reverse=True).collect(streaming=True)
shape: (49, 2)
┌─────────────┬─────────────┐
│ country     ┆ points_mean │
│ ---         ┆ ---         │
│ str         ┆ f64         │
╞═════════════╪═════════════╡
│ England     ┆ 92.888889   │
│ Austria     ┆ 89.276742   │
│ France      ┆ 88.92587    │
│ Germany     ┆ 88.626427   │
│ ...         ┆ ...         │
│ Brazil      ┆ 83.24       │
│ China       ┆ 82.0        │
│ Montenegro  ┆ 82.0        │
│ South Korea ┆ 81.5        │
└─────────────┴─────────────┘
```
Not much changes, except that in collect() we add: "streaming=True"
## **output**
We have got the output that we are looking for. Polars offers several ways to output our analyses, even to other formats useful for further analyses (e.g. pandas dataframe (`to_pandas()`) or numpy arrays (`to_numpy()`).
```python
# write the contents of lazy_df to a CSV file named "results.csv".
lazy_df.collect().write_csv('results.csv')
```
## **Conclusion**
**Polars** offers almost all the functions that we need to manipulate our dataframe. Next to that, it offers a lazy API that helps us optimising our queries before we execute them. Although we didn’t touch it is in this article, the benchmark of H20 shows that it is super efficient and fast. Especially with larger datasets it becomes worthwhile to look into the benefits that the lazy API has to offer.
## **List of consulted resources**  
* https://pola-rs.github.io/polars-book/user-guide/introduction.html  
* https://www.codemag.com/Article/2212051/Using-the-Polars-DataFrame-Library  
* https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html  
* https://www.notion.so/fr-fr/help/customize-and-style-your-content  
* https://blent.ai/git-tutoriel-complet/  
* https://github.com/pola-rs/polars  
* https://levelup.gitconnected.com/plodding-with-polars-in-python-defe8399eee6  
* https://r-brink.medium.com/introduction-to-polars-ee9e638dc163  
* https://www.kaggle.com/datasets/zynicide/wine-reviews?resource=download  
* https://r-brink.medium.com/2023-update-introduction-to-polars-c9250937604e  
* https://thinkr.fr/r-markdown-les-petits-trucs-qui-changent-la-vie/#:~:text=Faire%20un%20saut%20de%20ligne,-Pour%20faire%20un&text=%2D%2D%202%20espaces%20%C3%A0%20la%20fin,pas%20%C3%A0%20la%20ligne%20suivante.  
* https://nskm.xyz/assets/09-pandas.pdf  
* https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html  

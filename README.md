### README.md

Below is an example of how you might update the `README.md` file to include information about the `DataFrameProcessor` and `PropertyPricePredictor` classes, as well as the purpose of the `PNG_DIR` directory.

---

# UK House Prices Distribution

## Overview

This project analyzes UK house prices using Spark and various machine learning models to predict future trends. The results are visualized and saved as PNG files in a designated directory.

## Project Structure

```
/home/hduser/dba/bin/python/property_market_trends/
├── LICENSE
├── README.md
├── __init__.py
├── conf
├── data
├── deployment
├── designs
│   └── png_files
├── linux
├── othermisc
├── setup.py
├── sparkutils
├── src
│   └── uk_house_prices.py
├── tests
├── udfs
└── work
```

## Usage

To run the project, clone the repository and execute the main script located in the src directory. Detailed instructions are provided in the README.md.


```bash
git clone https://github.com/michTalebzadeh/property_market_trends.git
cd property_market_trends

Running the Script

Run the script using Python 3:

python3 src/uk_house_prices.py

## Classes

### DataFrameProcessor

This class is responsible for loading data from Hive, performing initial data analysis, and generating some basic plots.

#### Methods

- `__init__(self, spark, data)`: Initializes the `DataFrameProcessor` with a Spark session and data.
- `load_data(self)`: Loads data from a Hive table into a Spark DataFrame.
- `freedman_diaconis_bins(self, data)`: Calculates the optimal number of bins for a histogram using the Freedman-Diaconis rule.
- `functionLorentzian(self, x, amp1, cen1, wid1, amp2, cen2, wid2, amp3, cen3, wid3)`: Defines a Lorentzian function.
- `write_summary_data(self, df, tableName)`: Writes summarized data to a Hive table.
- `analyze_uk_ownership(self, house_df_final)`: Analyzes and plots UK house price data.

### PropertyPricePredictor

This class is responsible for training machine learning models to predict future house prices and generating plots.

#### Methods

- `__init__(self, data)`: Initializes the `PropertyPricePredictor` with data.
- `train_and_predict(self, RegionName, column_name)`: Trains machine learning models and generates predictions for a specified region and column.

## Directory for PNG Files

The directory for saving PNG files is set to `/home/hduser/dba/bin/python/property_market_trends/designs/png_files`. If this directory does not exist, it will be created automatically by the script.

### Example

Here is an example of how to use the classes in the script:

```python
import os
import json
import datetime
from pyspark.sql import SparkSession
from uk_house_prices import DataFrameProcessor, PropertyPricePredictor, create_png_dir

def main():
    appName = "UK House prices distribution"
    spark = SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    with open('/home/hduser/dba/bin/python/genai/data/property_data.json', 'r') as f:
        data = json.load(f)

    df_processor = DataFrameProcessor(spark, data)
    house_df = df_processor.load_data()

    total_data_count = house_df.count()
    print(f"Total data count: {total_data_count}")

    df_processor.analyze_uk_ownership(house_df)

    RegionName = 'Kensington and Chelsea'
    predictor = PropertyPricePredictor(data)
    predictor.train_and_predict(RegionName, 'Average Price')

    spark.stop()

if __name__ == "__main__":
    create_png_dir()
    start_time = datetime.datetime.now()
    print("PySpark code started at:", start_time)
    main()
    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print("PySpark code finished at:", end_time)
    print("Execution time:", execution_time)
```

## Dependencies

- Python
- PySpark
- Pandas
- Matplotlib
- lmfit
- scikit-learn
- statsmodels

Install the required Python packages using pip:

```bash
pip install pyspark pandas matplotlib lmfit scikit-learn statsmodels
```

## License

This project is licensed under the MIT License.

---

Feel free to customize the `README.md` content to better match your project's specifics and any additional details you want to include.


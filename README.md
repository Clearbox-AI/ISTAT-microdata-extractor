# ISTAT Microdata Extractor – Aspetti della Vita Quotidiana (AVQ)

This project provides tools for navigating and processing the [ISTAT microdata](https://www.istat.it/microdati/aspetti-della-vita-quotidiana/) from the survey **"Aspetti della Vita Quotidiana" (AVQ)**. It includes a Python class `ISTATMicrodataExtractor` with structured methods to explore, query, and analyze the AVQ dataset efficiently.

## 📦 Project Structure

The central component is the `ISTATMicrodataExtractor` class, which offers:

- 🚀 Simplified access to the dataset structure
- 🧠 Attribute encoding utilities
- 🔎 Filtering and pairing logic for household members
- 📊 Joint and conditional distribution tools
- 📁 Integration-ready design for larger analytical pipelines

## 📚 Dataset Overview

**Aspetti della Vita Quotidiana (AVQ)** is an annual survey by ISTAT capturing detailed aspects of daily life in Italian households. It includes information on:

- Demographics
- Education and employment
- Health and access to services
- Household composition and living conditions
- Digital device usage and internet access
- Family dynamics and caregiving
- Purchase habits

## 🧩 Key Features of `ISTATMicrodataExtractor`

| Method/Attribute                | Description                                                                |
|---------------------------------|----------------------------------------------------------------------------|
| `load_data()`                   | Loads and prepares the AVQ microdata from raw files                        |
| `attribute_categories`          | Attribute that contains all the categories for the attributes              |
| `get_attribute_metadata()`      | Retrieves metadata/encodings for categorical variables                     |
| `get_attributes_by_categories()`| Filters attributes by categories                                           |
| `filter()`                      | Applies logical filters on individual-level records                        |
| `pair_family_members()`         | Pairs individuals within the same household according to flexible rules    |
| `joint_distribution()`          | Computes joint/marginal distributions for selected variables               |


### Installing & Setup

```bash
git clone git@github.com:Clearbox-AI/ISTAT-microdata-extractor.git

pip install -r path/to/ISTAT-microdata-extractor/requirements.txt

pip install -e path/to/ISTAT-microdata-extractor
```

To setup your **AVQ ISTAT Microdata**, unzip the data folder you find [here](https://github.com/Clearbox-AI/ISTAT-microdata-extractor/tree/main/data) and provide the path to the unzipped folder to the `load_data()` method of your `ISTATMicrodataExtractor` class.

### 📊 Examples
```python
from microdata_extractor import ISTATMicrodataExtractor

# Supposing your AVQ Microdata ISTAT is stored in "AVQ_2022_IT"
avq = AVQMicrodatiISTAT()
avq.load_data("AVQ_2022_IT")

# Consult the available attribute categories 
avq.attribute_categories

# Filter attributes by relevant categories
_ = avq.get_attributes_by_categories("demographics","sport", "health_conditions", condition="or")

# Check encodings for categorical variables
encoding = avq.get_attribute_metadata("FREQSPO", print_output=True)

# Filter main dataset based on user-defined rules
# Tuples within the same inner list are AND-ed, tuples belonging to different inner lists are OR-ed
# The following rules express: (age>=18 AND BMI<=3)  OR  (age<18 AND BMIMIN==1)
rules = [
    [("ETAMi",">=",7),("BMI","<=",3)],  # Adults (age>=18) AND BMI==[1,2,3]
                                        # OR
    [("ETAMi","<",7),("BMIMIN","==",1)] # minors (age<18) AND BMIMIN==1
]

df_filtered = avq.filter(rules)
```

Check out the [Examples folder](https://github.com/Clearbox-AI/ISTAT-microdata-extractor/tree/main/Examples) for more!

### Contacts

📧 info@clearbox.ai
🌐 [www.clearbox.ai](https://www.clearbox.ai/)
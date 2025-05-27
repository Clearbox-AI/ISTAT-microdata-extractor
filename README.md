# ISTAT Microdata Extractor – Aspetti della Vita Quotidiana (AVQ)

This project provides tools for navigating and processing the [ISTAT microdata](https://www.istat.it/microdati/aspetti-della-vita-quotidiana/) from the survey **"Aspetti della Vita Quotidiana" (AVQ)**. It includes a Python class `AVQMicrodatiISTAT` with structured methods to explore, query, and analyze the AVQ dataset efficiently.

## 📦 Project Structure

The central component is the `AVQMicrodatiISTAT` class, which offers:

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

## 🧩 Key Features of `AVQMicrodatiISTAT`

| Method/Attribute                | Description                                                                |
|---------------------------------|----------------------------------------------------------------------------|
| `load_data()`                   | Loads and prepares the AVQ microdata from raw files                        |
| `attribute_categories`          | Attribute that contains all the categories for the attributes              |
| `get_attribute_encoding()`      | Retrieves metadata/encodings for categorical variables                     |
| `get_attributes_by_categories()`| Filters attributes by categories                                           |
| `filter()`                      | Applies logical filters on individual-level records                        |
| `pair_family_members()`         | Pairs individuals within the same household according to flexible rules    |
| `joint_distribution()`          | Computes joint/marginal distributions for selected variables               |


### Install dependencies via:

```bash
pip install -r requirements.txt
```

### Contacts

📧 info@clearbox.ai

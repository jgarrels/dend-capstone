# Travely Data Lake
## Data Engineering Capstone Project

### Project Summary
This project implements a Data Lake on S3 in parquet and json format, using mainly pandas, PySpark, and the AWS CLI. The data used includes datasets on US immigration, US demographics, worldwide daily temperatures, and airports.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Getting started
For an interactive run through the project, go through capstone_notebook.ipynb. If you just want to run the ETL process, use capstone.py. Please make sure to read "Prerequisites" right below this paragraph before running.

## Prerequisites

Before running the Jupyter notebook or the Python script, please
- make sure the Python packages required are installed - all imports are in the first cell below.
- make sure you have the AWS CLI installed. If you are on a Linux machine (like the VM provided in the Udacity workspace), you could use the `aws_cli_linux_install.sh` script (by running the command `bash aws_cli_linux_install.sh`). You can test if your aws cli works by running `aws --version` in a terminal.
- insert your AWS credentials, i.e. access key and secret key, in the `aws.cfg` file for the corresponding variables (without quotes around them). The IAM user you use needs to have full S3 permissions.
- create an S3 bucket you would like to write the parquet and json files to, and insert its name as the `DEST_BUCKET` variable in the `aws.cfg` file. Example name: `s3://travely-data-lake`

## Step 1: Scope the Project and Gather Data
### Scope of the project
* Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc.

Travely, a US-based touristic tour provider, would like to analyze data of people traveling to the US by plane. They would like to improve their offerings of day-tours and longer guided travels to meet their potential customers' needs. Additionally, they would like to know more about where people arrive and which months are heaviest in travelling so they can best advertise accordingly.

For this, they would like to have a data lake including data on people flying in to the US, the length of their stay, the airports and the respective cities and the weather on days with many arrivals. They want it to be accessible mainly to their data science team, who are all well-versed with Spark SQL, and it should be as inexpensive as possible for the time being.

For this project, I will use PySpark to process the data and AWS S3 to store it in parquet format.

### Describe and Gather Data
* Describe the data sets you're using. Where did it come from? What type of information is included? 

I am using the datasets provided by Udacity. These include:

**US immigration data**: A dataset that includes flight passenger data collected at immigration, such as the airport, arrival and departure, birthyear, gender, airline, etc.

**Airport codes**: A dataset about airports, including their international and local codes, country, municipality, coordinates etc.

**City temperature data**: A dataset about temperature in global cities, including data from the 18th to the 21st century, such as city, country, coordinates, temperature and temperature uncertainty.

**US cities demographic**: A dataset about US cities, including the state, total population, and other factors such as average household size.

### Step 2: Explore and Assess the Data
#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc. - done separately for datasets, in the ETL process. See "Explanation" part below.

#### Cleaning Steps
Document steps necessary to clean the data - done separately for datasets, in the ETL process. See "Explanation" part below.

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model

I chose a snowflake model with a fact-table (immigration) and several dimension tables (demographics, airport_codes, temperature, several desc tables) that can be joined onto it and some even onto each other. Possible JOINs are indicated with -> other_table.column_name.

immigration:
- year
- month
- airport_code
- address
- visacode
- biryear
- gender
- visatype
- stay_duration

temperature:
- dt
- avg_temperature
- avg_temperature_uncert
- city -> airport_codes.municipality, demographics.city
- country
- latitude
- longitude

airport_codes
- airport_type
- airport_name
- elevation_ft
- iso_country
- iso_region
- municipality -> demographics.city, temperature.city
- airport_code -> immigration.airport_code
- coordinates

demographics:
- city -> airport_codes.municipality, temperature.city
- state
- median_age
- male_population
- female_population
- population
- veterans
- foreign_born
- avg_household_size
- state_code

address_desc:
- key -> immigration.address
- value

airport_desc:
- key -> immigration.airport_code
- value

visacode_desc:
- key -> immigration.visacode
- value

A data dictionary can be found in `datadict.md`.

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

For each of the datasets, the pipeline process is either:
**SAS-format data (immigration dataset)**
* read data into Spark DataFrame
* explore data, identify data quality issues
* clean data, select relevant columns, rename columns to match snakecase_convention and to ease JOINing tables together
* write data to parquet files locally
* copy data to S3 using the AWS CLI

**CSV-format data (demographics, airport, temperature datasets)**
* read data into pandas DataFrame
* explore data, identify data quality issues
* clean data, select relevant columns, rename columns to match snakecase_convention and to ease JOINing tables together
* read the prepared data from the pandas DataFrame into a Spark DataFrame
* write data to parquet files locally
* copy data to S3 using the AWS CLI

**Why did I choose these steps?**
see Step 5 - rationale

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model - done separately for datasets, in the ETL process. See "Explanation" part below.

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks - done separately for datasets, in the ETL process. See "Explanation" part below.

#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

### Step 5: Complete Project Write Up
#### Clearly state the rationale for the choice of tools and technologies for the project.

* **pandas DataFrames** offer very fast data processing in tables, especially with csv as data source, as well as integrated table-displaying abilities.
* **PySpark** offers great Big Data processing abilities, a SQL API and a great range of options to adapt to many data formats. At scale, it also provides amazing distributed computing capabilities, which makes this solution easily scalable.
* **AWS S3** is a very inexpensive Cloud storage service with high-availability and no big up-front costs due to its pay-only-what-you-use model. It is well integrated with other AWS services, such as EMR, which could be a good option for scaling the project up (as discussed below in the part about the three scenarios).
* **Apache Parquet file format** is part of the Hadoop ecosystem, a very important Big Data ecosystem that Spark is part of as well. Parquet is a columnar storage which has several advantages over other formats like csv, including higher performance, only reading the minimum required amount of data, support of compression, and lesser size. This often results in faster queries and computations, less storage used, and thus fewer costs. This is ideal for data scientists using Spark to query the datasets. (Source and more details: https://databricks.com/glossary/what-is-parquet)
* **writing locally and then copying to S3**: While doing this project, I found that writing directly to S3 was a) very slow and b) used a lot of PUT, LIST etc. requests (which are more expensive than GET requests). Thus I decided to write the (in comparison to the CSV and SAS files) small parquet files to the local Udacity workspace and then copy them to S3. The results were astonishing, as the immigration data example shows: Writing the parquet files directly to S3 had not finished even after 6 hours, while first writing them locally and then copying to S3 took only around 15 + 2 min (yet always under 20 min).

For the reasons listed above I think that my technology choices are very well adapted to the problem presented. For scaling options, see the part about the three scenarios below.

#### Propose how often the data should be updated and why.
The data I used in this project does not have immediate updates. For Travely's immediate needs, no update is necessary for the time being.

Depending on the data source, one might consider different options as listed below:
* **Immigration data from the US National Tourism and Trade Office**: has different (paid!) dataset subscriptions
* **Temperature data**: The dataset comes from https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data, which apparently is not updated anymore. The original data comes from http://berkeleyearth.org/data/, where the data seems to be updated at least yearly.
* **Demographics data**: comes from https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/ and has not been updated since 2017. Thus, this might need to be replaced with or enriched with a more current and/or regularly updated dataset.
* **Airport data**: According to the source (https://datahub.io/core/airport-codes#readme), this data is updated on a (near-)daily basis.

Overall, if all of these sources were to be updated as often as possible when new data comes in, the airport dataset would prevail with a daily update. The best option would be to move the project to Airflow if updates should be made, and there one could schedule updating tasks according to the updating frequency of each data source separately.


#### Write a description of how you would approach the problem differently under the following scenarios:
 * **The data was increased by 100x**:
 In this case, I would consider moving the entire project to AWS EMR. EMR provides high-powered clusters for technologies like Hadoop and Spark, with the respective packages already installed. EMR offers integrated support for Jupyter notebooks as well as other AWS services, such as S3. You can submit a Spark job to the cluster and choose whether the cluster should keep running or terminate itself after the Spark job has finished.
 
 * **The data populates a dashboard that must be updated on a daily basis by 7am every day**:
 In this case, I would migrate the project to an Apache Airflow pipeline, which offers the option of regularly running certain steps, e.g. data ingestion, and also has the option to retry multiple times if a step fails.
 
 * **The database needed to be accessed by 100+ people**:
 In this case, I would consider different options of Data Warehouses or Database Services, such as AWS's Redshift or RDS (Relational Database Service), since probably some of these people would not be as well-versed in Spark.
 Another option would be ingesting the data into a dashboard or a BI application, depending on who these 100+ people are and in what format they can work best with the data.
 
 ## Explanation
Some of these steps are done for each dataset as it is processed - this is to avoid "hopping around" between the datasets.
You can best understand these steps if you go through the Jupyter notebook.


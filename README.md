# **Section 1: Data Pipeline**
#### _Transforming local data at fixed interval_ 

## 0. Probelm Statement
The objective of this section is to design and implement a solution to process a data file on a regular interval (e.g. daily). Assume that there are 2 data files dataset1.csv and dataset2.csv, design a solution to process these files, along with the scheduling component. The expected output of the processing task is a CSV file including a header containing the field names.

You can use common scheduling solutions such as cron or airflow to implement the scheduling component. You may assume that the data file will be available at 1am everyday. Please provide documentation (a markdown file will help) to explain your solution.

Processing tasks:

Split the name field into first_name, and last_name
Remove any zeros prepended to the price field
Delete any rows which do not have a name
Create a new field named above_100, which is true if the price is strictly greater than 100

To put things into context, the description of the challenges state the need to extract data from the 2 CSV files and apply some transformation before loading it together in one file. This documentation will break down into the following components:

1.  Assumption
2.  Instruction
3.  Explanation

I will first set the context with the assumption involved so I can implement what I believe to be the best solution. Then I will explain how to use the files in the repository to replicate the result as shown. Lastly, I will explain how I implement the solution and the reason behind. 

## 1. Assumption
##### Metadata
The challenge did not specify the metadata of the files, therefore I assume that all the files landed will contains a header with comma set as the delimiter. The data will also too contain strictly 2 columns. The name of the file is assummed to be the same for all instances. However, in the event the filename needs to be changed, there is a config file to make any changes.

##### Location of the Files
The challenge did not describe the location of the file, and by applying Occam's Razor I assume the file to be in local directory. Similarly the challenge did not describe the destination of the processed data, I too assume the file to be placed at in local directory too. However, in the event that the location of the files is different, there is a config file to make any changes.

##### Status of the Transformed data
The transformed file is assumed to be persistent in the directory, future data will be appened onto the older data. 

##### Size of the Files
The size of the data for both files stands at 5000 rows each, so I assume the pipeline I have to build will only need to handle data of a similar size for each batch.

##### Number of Files
The number of files is assumed to be 2 for all times. If there is only 1 file landed, the pipeline will still be able to process the remaining file. If there is a 3rd file the pipeline will ignore the 3rd file.

##### Operating System
The challenge did not mention about the environment, so I assumed the system is able to handle "Bash" commands. "Bash Operators" are used to remove files to prepare for future landings.

##### Airflow
I assumed that Apache Airflow is installed in the local machine to facilitate the regular processing of the files.

## 2. Instruction
##### Pre-requisite
In order to employ the pipeline, [Apache Airflow](https://airflow.apache.org/) have to be installed in the local machine.

In the Directed Acyclic Graph (DAG) file, I have employed these libraries and they have to be installed along with Airflow:
- Pandas
- Dateutil

The requirements are described in the [requirements.txt](https://github.com/xingyuan-tan/PostgreSQL-Demo/blob/main/requirements.txt) file.

##### Get started
1. Place the DAG file ([pipeline_transformation.py](https://github.com/xingyuan-tan/PostgreSQL-Demo/blob/main/pipeline_transformation.py)) in the DAGs folder for your Airflow
2. Configure the extraction and destination directory in the config file ([pipeline_transformation.cfg ](https://github.com/xingyuan-tan/PostgreSQL-Demo/blob/main/pipeline_transformation.cfg)) and place it with the DAG file
3. Start Airflow and enable the DAG for the pipeline to start extracting file from the target folder at every day 1.15 am. 


## 3. Explanation
The DAG will first start loading all the data into a Pandas DataFrame. There are 2 Python Operator working concurrently to hasten up the procedure. Since they are working independently, I did not get both the operator to place the transformed data in the same file. This is to avoid both of them accessing the file at the same time. Once the transformation is completed, I have another Python operator to merge both CSV files. Lastly, to prepare for future landing, I have to delete the initial files and the intermidary files. This is to avoid facing the issue on how to deal with the old file when the new file is landed. This can be further discuss with the upstream data flow on how to deal with unexpected scenario (eg. extended late landing). The DAG starts time is 15 mins later so to ensure all the lands properly just in case of minor delay.

##### Too large to be loaded on the Data Frame?
In this DAG, all the data is loaded before transformation. If the data size is too big, the system may not be able to completely load all the data. Another method, which is not demostrated since there is no need, is to read the data and process it row by row with a generator. This is avoid overloading the system when loading the data onto the dataframe. 

##### Too Slow to be processed?
However, this may be very slow for extremely large data set. To tackle this problem, each data set can be split into n-number of groups via their row's multiples and further split each operator to n-number of worker nodes. Ultimately the number of worker nodes, which is limited by the machine's capacity, will determine the limit of this potential pipeline.


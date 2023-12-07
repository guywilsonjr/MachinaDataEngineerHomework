# MachinaDataEngineerHomework
## Important frameworks
### [Pandas](https://pandas.pydata.org/)

Pandas is a python library that provides data structures and data analysis tools. Used widely throughout this codebase
to manipulate dataframes and perform calculations on the data.

### [Dask](https://dask.org/)

Dask is a python library that provides parallel computing capabilities. It's used to take advantage of multiple cores
on a machine to speed up the data processing pipeline.

## Requirements
- Python 3.11
- Writeable File System. The script will create a folder called `output` in the root directory of the project and store
the output files there.

## Instructions to Run
1. Clone the repository
2. Create a python 3.11 virtual environment
3. Activate the virtual environment
4. Install the requirements
```bash
pip install -r requirements.txt
```

5. Run the script
```bash
python main.py
```

### Pipeline process
#### 1. Data Ingestion and transformation
1. Read the data from the csv file
2. Cast the data to the correct data types
3. Separate the resulting dataframe by creating new dataframes for each run_uuid in the dataset
4. For each run_uuid dataframe, Run the following process:

#### 1.1. Data Augmentation
Convert the data to a wide format using the pandas pivot function and sort by time. One thing that I noticed is that
when you separate the data by run_uuid, some of the runs do not report any data for certain fields ex) x_1, fx_1, etc.
Therefore the pivot function will not create columns for these fields nor their derivative engineered fields. 
This means that each final run_uuid dataframe may have different columns. This is not a problem as the data is
separated and stored by run_uuid.

#### 1.2. Preprocessing
Here we use the data in a sorted by time and assume the most recent measurement for each columns is an acceptable
substitute for missing values. With this assumption, we can use the pandas `ffill` and `bfill` function to first fill
in the missing values with the most recent value for a datapoint with missing data. If there is no previous value for a 
missing data point, we assume that the next value is a good substitute and use the `bfill` function to fill in the missing values.

#### 1.3. Feature Engineering

Calculate the velocity, acceleration, and position

In parallel, calculate the following Data Frames:
1. `velocity`
2. `total_force`
3. `position`

Add the columns calculated above to the original dataframe by merging the dataframes which all have the `time` column
in common.

4. The `acceleration` column could not be calculated in parallel as it requires the velocity column to be calculated first.
We calculate acceleration as the change in velocity over change in time. Another way to calculate acceleration is to use
Acceleration = (2*(Distance - Velocity * Time) / Time^2). We chose the first method as it is more simple and has less
risk of numerical errors, especially considering a division function with very small numbers.

#### 1.4. Data Export
Export to a csv file in the `output` folder with the name of the file being the `run_data_[RUN_UUID].csv` of the data
that was processed including the new columns calculated in the feature engineering step.

#### 1.5. Generate Run Report
Generate a report for each run_uuid with the following information:
1. Run UUID
2. Start Time
3. End Time
4. Total Time
5. Total Distance For Robot 1 if applicable
6. Total distance for Robot 2 if applicable

#### 2 Create Final Report Summarizing All Runs
Create a final report summarizing all runs as a csv file in the `output` folder with the name `run_summary.csv` with
columns representing the data in step 1.5 each as a row in the csv file.

#### Notes
If more time were available, I would have liked to add unit tests to the codebase to ensure that the code is working
as expected. I would also like to add more error handling to the codebase to ensure that the code is robust and can 
handle unexpected inputs. While the code here scales well for data of the size in this example, it could be improved 
to handle larger datasets than can fit in memory. This could be done using dask and orchestrating distributed computation
or by processing the data in chunks.

Overall, it was a pleasure working on this project and I hope that you enjoy reviewing it. Thank you for your time.

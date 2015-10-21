# README for NIST Data Science Pre-Pilot Cleaning Task Baseline Systems

This README contains brief descriptions and usage instructions for the two baseline systems provided for the cleaning task of the NIST Data Science Pre-Pilot Evaluation.

The no information system is baseline-noinfo.py, and can be executed using the run\_serial\_baseline\_noinfo.sh or run\_parallel\_baseline\_noinfo.sh files. The .sh files may need to be adapted to the filepaths on the computer running the systems.

The reference system is baseline-referenceA.py and can be executed using the run\_serial\_baseline\_referenceA.sh or run\_parallel\_baseline\_referenceA.sh files. The .sh files may need to be adapted to the filepaths on the computer running the systems.

# Running the Baseline Systems

Both the no information and the baseline systems can be run in the same way: simple execute the relevant .sh file and it will run the .py file for each month of data on the cleaning task. When finished, the results will be in a directory, in the desired submission format. 

For instance, to run the no-information baseline system using the parallel implementation, execute

```bash
cd <directory with executable>
./run_parallel_baseline_noinfo.sh
```

It is likely that the paths to the data will need to be changed to where the data is. These files assume that the data are stored locally, but the files can be changed to point to the data on AWS if needed. 

# Baseline System Descriptions

Brief descriptions of how both baseline systems work are in the sections below.

## Algorithm Descriptions

The no-information system simply determines that each value is correct as is, believing that each data point is clean. Thus, it simply returns the provided flow values in a file format acceptable for submission.

The reference system uses a two-sided median.  After separating the data by detector and sorting by time for each lane detector, it examines each point and compares it to the value of the local median of the 9 local points (the selected point, the 4 previous points, and the 4 next points). If the difference between the current point and the median exceeds one standard deviation of the traffic flow values, then the point is corrected to be the value of the local median. The reference system assumes that the boundary points are automatically clean. The number of localized points considered and the acceptable difference in noise before correction are both parameters that can be changed in the system.

## Implementation Software Description

Both systems process one file at a time. Each baseline system is implemented in Python 2.x, using the pandas library.  The python files containing the system take in one lane detector file for one month of one year and produce the cleaned submission for that one month of data. To run multiple files, one can either run them in sequence such as piping the data files to the python files through xargs, or through using GNU Parallel (http://www.gnu.org/software/parallel/), one can execute the cleaning of each month in parallel.

# Contact

Contact datascience@nist.gov if there are any questions concerning the baseline systems.
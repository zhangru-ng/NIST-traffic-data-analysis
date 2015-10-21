# NIST traffic data analysis
##Cleaning Task
**Description**: participants are asked to clean traffic lane detector measurements containing incorrect
flow values, providing correct traffic flow values for the erroneous traffic flow measurements.


**Input**: traffic lane detector measurements with erroneous traffic flow values and speeds (in number
and average speed of vehicles since the last interval).


**Output**: cleaned traffic lane detector measurements with cleaned traffic flow values (in number of
vehicles since the last interval).


**Extra comments**:
  * The only data available for this task include both development data and testing data.
  * Both **“speed”** and **“flow”** (please ignore “occupancy” or “quality” fields) values can have errors.
  * Make use of the given testing data (lane_id, measurement_start, speed and flow) and optionally development data, then output the correct “flow” value for each row of testing data


**Constraints**:
  * In a period of time, flows in the same zone of different lanes should have similar
values.
  * In a period of time, flows of nearby zones should be similar.
  * Flow values must be nonnegative numbers.
  * Measurements of (flow, speed, occupancy) should be consistent.
  * More


**Correct extraneous flow values**:
  * Correct the values as values of nearby location or time.
  * Correct the values as output of a regression model F(location,time) -> flows. The F is learned from the dirty data.

**Implementation framework**:
  * mapreduce framework.
  * how to store the bulky data for efficient access (e.g. distributed access, minimum data transfer, in-memory caching, indexing, etc).
  * how the system pipeline facilitates solving the problem (e.g. minimize the number of passes of the data, design end-to-end system without much manual work to redirect input/output in between).
  
  

To learn about the challenge, please read Challenge.md

Usage:

python [ NumpyETL.py | PySpark.py ] \<Number of Slices> \<Debug Flag>

NumpyETL.py: Use Numpy to process data

PySpark.py: Use PySpark to process data

Number of Slices: Slice the data used into a number of batches to be processed at once. 
Used for PySpark only. The more processing power, the higher the value. 
Default is 800

Debug Flag: Used for displaying tables, off by default. 

Restrictions:

* The required csv files (candidates, committees, individual_contributions, industry_codes)
must be in a sub-directory name data.

* This code has been implemented and tested on a system with limited resources
(no access to a better machine, cloud services are too expensive). Because of this I have 
not been able to scale to gigabytes of data (more specifically, individual contributions)
as memory issues begin to appear. Please be aware of this if you decide to run a large
amount of data.

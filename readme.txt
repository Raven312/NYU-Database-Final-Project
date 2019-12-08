### Project title
----------------
For this project, we will run the sql function program in a miniature relational database with the order by using Python.
The program mainly covers the basic operations of relational algebra such as
projection, count, sum, average, corresponding count group, sumgroup avggroup, aggregates, group by, and join function.
Our system includes direct and clean data that extract the required information from the required tables.
For the main execution of the project, we handle two files: sales1 & sales2 in the miniature relational database
to test its efficiency and run time via python. Further, we optimize the required sql function code through Python
to observe the performance and whether it can execute correctly and concisely.


### Tests and output
----------------
cd to the project home directory and put the test file: <test_file_name.txt> in the project home directory.
run the following command:
python3 main.py <test_file_name.txt>
# The test file should only contain the sql command without any useless empty line.

The output file will be generated at the directory: /output
the file name is defined by the sql: outputtofile(<table_name>, <output_file_name>.txt)


### Commands
-------------
Our database system uses Object-Oriented Programming (OOP) concept for writing python queries.
Python, a widely used interpreted, high-level programming language fits well with its clarity and readability in this
project. Also, we uses classes to organize our code into generic, reusable peaces. That means we have the advantage
to use the code repeatedly with small modification. We follow the given standard input and run our programs on test cases
as below:

R := inputfromfile(sales1)
R1 := select(R, (time > 50) or (qty < 30))
R2 := project(R1, saleid, qty, pricerange)
R3 := avg(R1, qty)
R4 := sumgroup(R1, time, qty)
R5 := sumgroup(R1, qty, time, pricerange)
R6 := avggroup(R1, qty, pricerange)
S := inputfromfile(sales2)
T := join(R, S, R.customerid = S.C)
T1 := join(R1, S, (R1.qty > S.Q) and (R1.saleid = S.saleid))
T2 := sort(T1, S_C)
T2prime := sort(T1, R1_time, S_C)
T3 := movavg(T2prime, R1_qty, 3)
T4 := movsum(T2prime, R1_qty, 5)
Q1 := select(R, qty = 5)
Btree(R, qty)
Q2 := select(R, qty = 5)
Q3 := select(R, itemid = 7)
Hash(R,itemid)
Q4 := select(R, itemid = 7)
Q5 := concat(Q4, Q2)
C1 := count(R1, qty)
C2 := countgroup(R1, time, qty)
C3 := countgroup(R1, qty, time, pricerange)

### Format of .txt files
-----------------------
A .db files start with a line containing all column names (at least one) separated by bars.
For instance, the sales1 table shown as below:

saleid|itemid|customerid|storeid|time|qty|pricerange
36|14|2|38|49|15|moderate
784|90|182|97|46|31|moderate
801|117|2|43|81|14|outrageous
905|79|119|81|67|44|outrageous
227|68|2|66|67|42|supercheap
951|102|116|45|35|1|outrageous
492|100|2|7|67|39|outrageous
228|100|16|11|67|27|outrageous


### File list
-------------
File that includes with this projects:

__init__.py---export selected portions of the package under convenient name and hold convenience functions.

ConditionalObject.py---Represents a staticmethod for users to apply for specific execute requirements.

DatabaseFunction.py---Related Statements that performs specific tasks.

DbObject,py---DB object which store in the dictionary.

GeneralFunction.py---Self contained modules of code to accomplish specific tasks.

MyBTreeTable.py---Table in BTree Data Structure.

MyHashTable.py---Table in Hash Data Structure.

ParameterAssignmentTable.py---Detailed ParameterAssignment Table for specific tasks.

sales1.txt---RowData

sales2.txt---RowData

main.py: The main program entry point to the file data.

readme.txt---This file

test.txt---Subdirectory holding files for testing.


### Tech/framework used
-----------------------
Built with PyCharm IDE and Python version 3.7, mainly using python numpy, core language features, string manipulation,
and data structure in B-Trees & Hash, and OOP concepts.


### Project data source and UploadGuide
---------------------------------------
Project: A miniature relational database with order

https://cs.nyu.edu/cs/faculty/shasha/papers/sales1
https://cs.nyu.edu/cs/faculty/shasha/papers/sales2
Reprozip Instructions


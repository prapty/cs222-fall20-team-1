## Project 4 Report


### 1. Basic information
 - Team #:1
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-1
 - Student 1 UCI NetID: rprapty
 - Student 1 Name: Renascence Tarafder Prapty
 - Student 2 UCI NetID (if applicable): slimprap
 - Student 2 Name (if applicable): Siraphob Limprapaipong (Kop)


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns). 

We create a new catalog table for index. This table is called "Index_team_1".
It has columns: table name, attribute name, index name. We insert rows in "Tables" and "Columns" catalog tables for "Index_team_1" table.

While creating an index we insert a new row in "Index_team_1" table. 
While destroying an index, we delete corresponding row from "Index_team_1" table.

### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

  We start by getting the position of the given condition attribute in the record, then run through the dataset and compare data
  if it is accepted by the given condition or not, if it is then send it back to the caller and continue until the end of the dataset.

### 4. Project
- Describe how your project works.

  We start by getting the position map of the given condition attributes in the record, then run through the dataset and compare data
  column to only get the data in the given condition attributes. We continue until the end of the dataset and send back the data collected 
  to the user.

### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.

We start by getting the position of the given condition attribute in left iterator and right iterator.
Then we all tuples from left iterator one by one and for each tuple, 
initiate scan on right iterator. Then we read all tuples from right iterator. 

For each left tuple and right tuple pair, we check whether given condition is satisfied.  
If condition is satisfied, we merge left tuple and right tuple and return combined result.

Due to lack of time, we could not implement BNL using buffer.

### 6. Index Nested Loop Join
- Describe how your index nested loop join works. 

We start by getting the position of the given condition attribute in left iterator and right iterator.
Then we read condition attribute of all tuples from left iterator one by one and for each tuple, 
initiate an index scan for condition attribute and condition op on right iterator. Then we read all tuples from right iterator. 

We merge left tuple and right tuple and return combined result.


### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.

  We start by getting the position of the given attribute in the record, then using it to know which attribute to use in 
  the aggregation, then we run through the dataset and compute the given task ex. add all data together for SUM or 
  finding maximum value of the given attribute in the dataset.

- Describe how your group-based aggregation works. (If you have implemented this feature)
  
  

### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.

  -

- Other implementation details:

  -

### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.

    - rm.cc:
            - Both: 
            - Student 1: debugging, indexScan, createIndex, destroyIndex, getIndexAttributes, modifications of: createCatalog, deleteCatalog, createTable, deleteTable, insertTuple, updateTuple, deleteTuple
            - Student 2: getNextEntry, close()
    - qe.cc:
            - Both: debugging, Filter::getNextTuple, Project, BNL Join, BNL::getNextTuple
            - Student 1: Project::getNextTuple, INL Join
            - Student 2: Filter, getAttributes, Aggregate, Aggregate::getNextTuple     

### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)

  -

- Feedback on the project to help improve the project. (optional)

  -
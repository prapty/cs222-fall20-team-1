## Project 2 Report


### 1. Basic information
 - Team #:1
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-1
 - Student 1 UCI NetID: rprapty
 - Student 1 Name: Renascence Tarafder Prapty
 - Student 2 UCI NetID (if applicable): slimprap
 - Student 2 Name (if applicable): Siraphob Limprapaipong (Kop)

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.



### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

    Same with P1

- Describe how you store a null field.

    Same with P1

- Describe how you store a VarChar field.

    Same with P1

- Describe how your record design satisfies O(1) field access.

    Same with P1

### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

    We add tomb byte to tell if the record is moved elsewhere and if it is then we can get the data about new location via 
    offset byte (int which is the combination of short of pageNum and slotNum).

- Explain your slot directory design if applicable.

We have data about slot in the back of the page with the structure of
  record 1|record 2|....|record n| ... |tombstone of n|offset of record n|size to record n|.....|tombstone of 2|
  |offset of record 2|size of record 2|tombstone of 1|offset to record 1|size of record 1|number of record of this page|

### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

We have implemented dynamic hidden page management. Initially there is only one hidden page.
It is the first page of the file. After every 1500 record pages, we keep an empty page in case it is needed to be used as a hidden page.
We use as many hidden pages as are needed to store counter values, number of pages, number of hidden pages and page availability of each record page.


- Show your hidden page(s) format design if applicable

We store following five values in first 20 bytes of first hidden page:
readPageCounter|writePageCounter|appendPageCounter|numberOfPages|numberOfHiddenPages

We create following string for page availability:
page0availability.page1availability ... pagenavailability

If this string is less than 4076 bytes long, then it is padded at the end and saved in the first hidden page starting from position 21.

Otherwise, multiple substrings are generated. First substring is 4076 bytes long and it is saved in first page starting from 21 postion.
Second substring is 4096 bytes long or it is padded at the end to make so and saved in second hidden page which is 1501th page of file. Third substring is saved in 3001th page of file.

### 6. Describe the following operation logic.
- Delete a record

    We use given rid to find the target page and record. Then, we call moveRecord() to do the operation. First, we copy
    the number of record in the page into the new Page. After that, we loop all records with 3 condition:
    1. if it is the target page then change offset/size counter of that record to -1 in the new page.
    2. if it is not the target page and its offset is smaller than target page (no need to move) we copy its record data 
    and its offset/size counter into the new Page in the same location
    3. if it is hot the target and its offset is bigger than the target page, we copy it to a new location such that it
    moves to the left by length of target record and update its offset according to it (and size unchanged).

- Update a record

    We use deleteRecord and insertRecord on the target rid. After that we check the new location. If it is the same slot
    then don't change anything. If it is not the same slot but in the same page then change data at the end of the page
    to use the old slot. Else, use tomb byte to tell that it changes location and use int in size to tell the new location 
    via short of page + short of slot

- Scan on normal records

We do not read from disk for each record during scan. We maintain the current position of an iterator through a global rid variable inside the iterator. When current position of iterator is the first record of a page, we read the whole page from disk and keep it in memory.
We also store the total number of records in current page. After the first record, we read the rest of the records of current page from memory. 

For each record, we read condition attribute value and compare it with the provided value using given compOp. If codition is satisfied, we return this record. Otherwise, we go on to next record. If compOp value is NO_OP, we skip this check and directly return the record.

If we reach the end of file, we return EOF. We determine end of file by using current position and total number of pages of the file.

Before returning from getNextRecord, we increment the slotNum of current position by 1. If slotNum reaches the end of page, we increase pageNum by 1 and set slotNum to 0.

- Scan on deleted records

Every record has an offset in its slot. For a deleted record, offset has value -1. During scan, we skip a deleted record
by checking its offset value. If offset value is -1, we skip this record and go on to next record.


- Scan on updated records

Every record has a tombstone byte in its slot. For an updated record, if the record moves to a new page, tombstone value is 1 and pointer to new record location is stored in original record location. 
During scan, we skip an updated record's original location by checking its tombstone value. If tombstone value is 1, we skip this record and go on to next record.



### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.

    - rbfm.cc:
            - Both: debugging, discuss and change the data structure (record/page) for p2
            - Student 1: dynamic hidden page, readAttribute, scan, rbfm iterator getNextRecord, rbfm iterator close
            - Student 2: deleteRecord, moveRecord, tombstone byte, updateRecord, find_deleteRecord, findRecord
    - rm.cc:
            - Both: debugging
            - Student 1: deleteCatalog, updateTuple, deleteTuple, readTuple, printTuple, getAttributes, readAttribute, scan, rm iterator getNextTuple, rm iterator close
            - Student 2: createCatalog, insertTuple, getAttributesLength, append_buffer, deleteTable, insertAttributeTable, insertAttributeCol

### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 2, but not related to the other sections (optional)

  We have issue with auto-grading, which we tried to go to both TA/prof. office hr and spend days to fix but we cannot do it
  for read_large_tuple, update_large_tuple and scan_large_tuple although the testing on local works fine and we tried to
  print the result on auto-grading and it seems to be equal and by using strcmp we got 0 (it fails on memcmp of inBufer/outBuffer).

- Feedback on the project to help improve the project. (optional)

    If possible we want auto-grading to be easier to debug and having an instruction on how to have the exact environment
    for the code to run and get the same result. It is very hard to debug when it runs fine on anything but auto-grading 
    (both of our PC can run the test).
    
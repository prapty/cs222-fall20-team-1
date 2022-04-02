## Project 1 Report


### 1. Basic information
 - Team #: 1
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-1
 - Student 1 UCI NetID: rprapty
 - Student 1 Name: Renascence Tarafder Prapty
 - Student 2 UCI NetID (if applicable): slimprap
 - Student 2 Name (if applicable): Siraphob Limprapaipong (Kop)


### 2. Internal Record Format
- Show your record format design.
  
  We use variable length record format with basic structure as followed:
  |size of record|offset 1|offset 2|...|offset r|N_1|V_1|N_2|V_2|...|N_r|V_r|
  N_i from i = 1 to r is the size of each record
  V_i from i = 1 to r is the value of record
  
  We can go directly to the target field via offset at the front of the record.
  

- Describe how you store a null field.
  
  We use N_i in our design to indicate if V_i is a null value or not. We indicate a null field by giving N_i the value -1.

- Describe how you store a VarChar field.

  We start by finding the actual length of a VarChar field and then use it as N_i of our design, which will reduce the space
  needed for a VarChar field.

- Describe how your record design satisfies O(1) field access.

  We have offset at the end of each page to point at a specific slotNum and then we have offset for every nth field
  which we can use to access the nth field in o(1). 
    
### 3. Page Format
- Show your page format design.

  We use fixed length with each page size equal to PAGE_SIZE (4096 bytes in this case). We have the first page to be 
  the hidden page where it stores readPageCounter, writePageCounter, appendPageCounter, number of pages, and availability
  of free spaces in every page in the system. After the hidden page, we have pages with records. Record pages consist of record at 
  the front with number of records in the page at the end of the page. Each record will have offset and length at the end before
  the number of records.


- Explain your slot directory design if applicable.

  We have data about slot in the back of the page with the structure of
  record 1|record 2|....|record n| ... |offset of record n|size to record n|.....|offset of record 2|size of record 2|
  |offset to record 1|size of record 1|number of total record of this page

### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.
  
  we have a vector of int called pageAvailability that store data about free space of each page which then we can loop to see
  if the space is enough for the record. if none is available then we create a new page for this record.


- How many hidden pages are utilized in your design?

  One hidden page is utilised in our design. 

- Show your hidden page(s) format design if applicable

  We create a string in the following format:
  readPageCounter,writePageCounter,appendPageCounter,numberOfPages.page0availability.page1availability ... 
  pagenavailability.
  
  This string is padded at the end by spaces to make it 4096 bytes long. Then the whole string is stored in the hidden page.



### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.

    - pfm.cc:
        - Both: debug the code, discuss about conflict between pfm.cc and rbfm.cc
        - Student 1: readPage, writePage, appendPage, getNumberOfPages, getCounterValues, save counter values and number of pages to file, get counter values and number of pages from file
        - Student 2: createFile, destroyFile, openFile, closeFile
        
    - rbfm.cc:
        - Both: record format design, record storage design, getLength, debug all the functions
        - Student 1: printRecord, encodeRecord, decodeRecord
        - Student 2: createFile, destroyFile, openFile, closeFile, insertRecord, readRecord, nullIndicator
        
        

### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
        
       -If a test method crashes, the file is not destroyed. It cuases other test methods to also fail. So it will be helpful if following line is added at the beginning of SetUp() method in rbfm_test_utils.h:
                rbfm.destroyFile(fileName);// DELETE THIS
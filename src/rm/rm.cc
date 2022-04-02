#include "src/include/rm.h"
#include "src/include/rbfm.h"
#include "src/include/pfm.h"
#include <src/include/ix.h>
#include <cstring>
#include <bits/stdc++.h>

using namespace std;

PeterDB::RecordBasedFileManager *rbfm = &PeterDB::RecordBasedFileManager:: instance();
PeterDB::IndexManager *ix = &PeterDB::IndexManager:: instance();

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        //reset all data
        table_attribute.clear();
        col_attribute.clear();
        index_attribute.clear();
        tabMap.clear();
        colMap.clear();
        indexMap.clear();
        attributeMap.clear();

        catalogStatus = 1;
        //open file for Table and Col which should return -1 since it should not exists
        //if(rbfm->createFile(tableFileName) != 0 || rbfm->createFile(colFileName) != 0) {
        if(rbfm->createFile(tableFileName) != 0 || rbfm->createFile(colFileName) != 0||ix->createFile(indexFileName)) {
            return -1;
        }
        //create temp attribute for table
        getTablesAttributes(table_attribute);

        //create temp attribute for col
        getColumnsAttributes(col_attribute);

        //create temp attribute for index
        getIndexAttributes(index_attribute);

        int returnVal = createTable("Tables", table_attribute);
        if(returnVal == -1)
            return -1;
        returnVal = createTable("Columns", col_attribute);
        if(returnVal == -1)
            return -1;
        returnVal = createTable(indexFileName, index_attribute);
        return returnVal;
    }

    RC RelationManager::deleteCatalog() {
        catalogStatus = 0;
        //if(rbfm->destroyFile(tableFileName) == 0 && rbfm->destroyFile(colFileName) == 0) {
        if(rbfm->destroyFile(tableFileName) == 0 && rbfm->destroyFile(colFileName) == 0 && ix->destroyFile(indexFileName)==0) {
            return 0;
        }
        return -1;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        FileHandle fileHandle;
        RID rid;
        if(catalogStatus == 0){
            return -1;
        }
       // if(!(tableName == tableFileName||tableName == colFileName)){
        if(!(tableName == tableFileName||tableName == colFileName||tableName==indexFileName)){
            //if creating file if not systecreateTable-tablecountm file and if it is not successful return -1
            if(rbfm->createFile(tableName) != 0) {
                return -1;
            }
        }
        //insert table-id:int, table-name:varchar(50), file-name:varchar(50) into table file
        if(rbfm->openFile(tableFileName, fileHandle) == -1){
            return -1;
        }
        if(insertAttributeTable(table_count + 1, tableName, tableName, fileHandle, rid) == -1){
            return -1;
        }
        tabMap.insert({tableName, rid});
        if(rbfm->closeFile(fileHandle) == -1){
            return -1;
        }
        //insert table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int into col file
        int numberOfCol = attrs.size();
        if(rbfm->openFile(colFileName, fileHandle) == -1){
            return -1;
        }
        map<int, RID> tempMap;
        for(int i = 0; i< numberOfCol; i++){
            int colPos = i + 1;
            insertAttributeCol(table_count+1, attrs[i], fileHandle, colPos, rid);
            tempMap.insert({i,rid});
        }
        colMap.insert({tableName, tempMap});
        table_count++;
        return rbfm->closeFile(fileHandle);
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        FileHandle fileHandle;
        vector<Attribute> attribute;
        getAttributes(tableName, attribute);
        //you should not delete system file via deleteTable
        if(tableName == tableFileName || tableName == colFileName || tableName==indexFileName)
            return -1;
        //destroy the file of tableName
        if(rbfm->destroyFile(tableName) == -1){
            return -1;
        }
        //check each attribute and destroy corresponding index if applicable
        for(int i=0; i<attribute.size(); i++){
            Attribute a=attribute[i];
            string indexName=tableName+"_"+a.name+".idx";
            if(indexMap.find(indexName) != indexMap.end()){
                if(destroyIndex(tableName, a.name)==-1){
                    return -1;
                }
            }
        }
        //check if tableName exists in tabMap or not
        if(tabMap.find(tableName) != tabMap.end()){
            RID table_rid = tabMap[tableName];

            if(rbfm->openFile(tableFileName, fileHandle) == -1) {
                return -1;
            }

            if(rbfm->deleteRecord(fileHandle, table_attribute, table_rid) == -1){
                return -1;
            }
            rbfm->closeFile(fileHandle);
            tabMap.erase(tableName);
        }

        //check if tableName exists in colMap or not
        if(colMap.find(tableName) != colMap.end()){
            if(rbfm->openFile(colFileName, fileHandle) == -1) {
                return -1;
            }
            map<int, RID> column_rid = colMap[tableName];

            for(int i = 0; i < column_rid.size(); i++){
                rbfm->deleteRecord(fileHandle, col_attribute, column_rid[i]);
            }
            rbfm->closeFile(fileHandle);
            colMap.erase(tableName);
        }

        return 0;
    }

    RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
        FileHandle fileHandle;
        int returnVal = rbfm->openFile(tableName, fileHandle);
        if(returnVal==-1){
            return -1;
        }
        rbfm->closeFile(fileHandle);
        if(attributeMap.find(tableName) != attributeMap.end()) {
            attrs = attributeMap[tableName];
            return 0;
        }

        //string tableTableName="Tables";
        int tableId=getTableId(tableName);

        if(tableId != -1){
            returnVal=getColumnDetails(attrs, tableId);
            if(returnVal==-1){
                return -1;
            }
            attributeMap.insert({tableName, attrs});
            return 0;
            }

        return -1;
    }

    RC RelationManager::getColumnDetails(vector<Attribute> &attrs, int tableId) {
        PeterDB::RM_ScanIterator rmsiColumn;
        char *outBuffer = new char[PAGE_SIZE];
        RID rid;

        string columnTableName="Columns";
        vector<string> columnFetchAttributes;
        columnFetchAttributes.push_back("column-name");
        columnFetchAttributes.push_back("column-type");
        columnFetchAttributes.push_back("column-length");

        string columnConditionalAttribute="table-id";
        scan(columnTableName, columnConditionalAttribute, EQ_OP, &tableId, columnFetchAttributes, rmsiColumn);
        if(rmsiColumn.getNextTuple(rid, outBuffer) != RM_EOF){
            Attribute temp;
            //skip null indicator
            int outputOffset=1;
            int nameSize;
            memcpy(&nameSize, (char*)outBuffer+outputOffset, sizeof(int));
            outputOffset+=sizeof(int);

            char * nameValue = new char[nameSize];
            memcpy(nameValue, (char*)outBuffer+outputOffset, nameSize);
            outputOffset+=nameSize;
            string nameValueStr((char *)nameValue);
            if(nameValueStr.length()>nameSize){
                nameValueStr=nameValueStr.substr(0,nameSize);
            }
            temp.name=nameValueStr;


            memcpy(&temp.type, (char*)outBuffer+outputOffset, sizeof(int));
            outputOffset+=sizeof(int);

            memcpy(&temp.length, (char*)outBuffer+outputOffset, sizeof(int));

            attrs.push_back(temp);

            while(rmsiColumn.getNextTuple(rid, outBuffer) != RM_EOF){
                Attribute temp;
                //skip null indicator
                int outputOffset=1;
                int nameSize;

                memcpy(&nameSize, (char*)outBuffer+outputOffset, sizeof(int));
                outputOffset+=sizeof(int);

                char * nameValue = new char[nameSize];
                memcpy(nameValue, (char*)outBuffer+outputOffset, nameSize);
                outputOffset+=nameSize;
                string nameValueStr((char *)nameValue);
                if(nameValueStr.length()>nameSize){
                    nameValueStr=nameValueStr.substr(0,nameSize);
                }
                temp.name=nameValueStr;

                memcpy(&temp.type, (char*)outBuffer+outputOffset, sizeof(int));
                outputOffset+=sizeof(int);

                memcpy(&temp.length, (char*)outBuffer+outputOffset, sizeof(int));

                attrs.push_back(temp);
            }
            rmsiColumn.close();
            return 0;
        }
        return -1;
    }

    RC RelationManager::getTableId(const string &tableName){
        PeterDB::RM_ScanIterator rmsiTable;

        string tableTableName="Tables";
        vector<string> tableFetchAttribute{"table-id"};
        string tableConditionalAttribute="table-name";
        int returnValue=scan(tableTableName, tableConditionalAttribute, EQ_OP, tableName.c_str(), tableFetchAttribute, rmsiTable);
        if(returnValue==-1){
            return -1;
        }
        RID rid;
        char *outBuffer = new char[PAGE_SIZE];
        int tableId=-1;
        if(rmsiTable.getNextTuple(rid, outBuffer) != RM_EOF) {
            memcpy(&tableId, (char *) outBuffer + 1, sizeof(int));
            rmsiTable.close();
        }
        return tableId;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        FileHandle fileHandle;
        vector<Attribute> attribute_Table;

        //need to prepare attribute to use for insertion
        getAttributes(tableName, attribute_Table);

        int returnVal = rbfm->openFile(tableName, fileHandle);
        if(returnVal == -1)
            return -1;
        returnVal = rbfm->insertRecord(fileHandle, attribute_Table, data, rid);
        //printf("Insert record okay\n");
        rbfm->closeFile(fileHandle);
        if(returnVal==0){
            returnVal=indexOperations(INSERT_INDEX_ENTRY, data, attribute_Table, tableName, rid);
        }
        //printf("Insert Entry okay\n");
        return returnVal;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        FileHandle fileHandle;
        vector<Attribute> attribute_Table;

        //need to prepare attribute to use for insertion
        getAttributes(tableName, attribute_Table);

        //read record to remove entries from index
        char *readData=new char[PAGE_SIZE];
        int returnVal = readTuple(tableName, rid, readData);
        if(returnVal == -1)
            return -1;
        returnVal=indexOperations(DELETE_INDEX_ENTRY, readData, attribute_Table, tableName, rid);
        if(returnVal == -1)
            return -1;

        returnVal = rbfm->openFile(tableName, fileHandle);
        if(returnVal == -1)
            return -1;
        returnVal = rbfm->deleteRecord(fileHandle, attribute_Table, rid);
        rbfm->closeFile(fileHandle);
        return returnVal;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        FileHandle fileHandle;
        vector<Attribute> attribute_Table;

        //need to prepare attribute to use for insertion
        getAttributes(tableName, attribute_Table);

        //read current record to remove current entries from index
        void *readData=new char[PAGE_SIZE];
        int returnVal = readTuple(tableName, rid, readData);
        if(returnVal == -1)
            return -1;
        returnVal=indexOperations(DELETE_INDEX_ENTRY, readData, attribute_Table, tableName, rid);
        if(returnVal == -1)
            return -1;

        returnVal = rbfm->openFile(tableName, fileHandle);
        if(returnVal == -1)
            return -1;
        returnVal = rbfm->updateRecord(fileHandle, attribute_Table, data, rid);
        rbfm->closeFile(fileHandle);
        if(returnVal == -1)
            return -1;
        //inset updated entries in index
        returnVal=indexOperations(INSERT_INDEX_ENTRY, data, attribute_Table, tableName, rid);
        return returnVal;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        FileHandle fileHandle;
        vector<Attribute> attribute_Table;
        //need to prepare attribute to use for insertion
        getAttributes(tableName, attribute_Table);

        int returnVal = rbfm->openFile(tableName, fileHandle);
        if(returnVal == -1)
            return -1;
        returnVal = rbfm->readRecord(fileHandle, attribute_Table, rid, data);
        rbfm->closeFile(fileHandle);
        return returnVal;
    }

    RC RelationManager:: indexOperations(int operation, const void *inputData, const std::vector<Attribute> &attrs, const std::string &tableName, const RID &rid){
        int record_size = attrs.size();
        int inputOffset = 0;

        //skip null indicator in the beginning
        inputOffset += ceil(record_size / 8.0);

        //get null indicator fields
        int offset = ceil(record_size / 8.0);
        int isnull[record_size];
        for (int i = 0; i < offset; i++) {
            char checkNull;
            memcpy(&checkNull, (char *) inputData + i, 1);
            bitset<8> nullBitset(checkNull);
            for (int j = 0; j < 8 && 8 * i + j < record_size; j++) {
                isnull[8 * i + j] = nullBitset.test(7 - j);
            }
        }
        for (int i = 0; i < record_size; i++) {
            Attribute a = attrs[i];
            //index operations only for not null values
            if (isnull[i] == 0) {
                string indexName=tableName+"_"+a.name+".idx";
                IXFileHandle ixFileHandle;
                //index exists on this attribute
                if(indexMap.find(indexName)!=indexMap.end()){
                    ix->openFile(indexName, ixFileHandle);
                    switch (operation) {
                        case INSERT_INDEX_ENTRY:
                            ix->insertEntry(ixFileHandle, a, (char *)inputData+inputOffset, rid);
                            break;
                        case DELETE_INDEX_ENTRY:
                            ix->deleteEntry(ixFileHandle, a, (char *)inputData+inputOffset, rid);
                            break;
                    }
                    ix->closeFile(ixFileHandle);
                }
                if (a.type == TypeVarChar) {
                    //skip length
                    inputOffset += sizeof(int);
                    //skip value
                    int actualLength;
                    memcpy(&actualLength, (char *)inputData+inputOffset, sizeof(int));
                    inputOffset+=actualLength;
                }
                else{
                    //skip value
                    inputOffset += sizeof(int);
                }
            }
        }
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return rbfm->printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        FileHandle fileHandle;
        vector<Attribute> attribute_Table;

        //need to prepare attribute to use for insertion
        getAttributes(tableName, attribute_Table);

        int returnVal = rbfm->openFile(tableName, fileHandle);
        if(returnVal == -1)
            return -1;
        returnVal = rbfm->readAttribute(fileHandle, attribute_Table, rid, attributeName, data);
        rbfm->closeFile(fileHandle);
        return returnVal;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        FileHandle fileHandle;
        vector<Attribute> attribute_Table;

        //need to prepare attribute to use in rbfm scan
        if(tableName.compare("Tables")==0){
            //getTablesAttributes(table_attribute);
            attribute_Table=table_attribute;
        }
        else if(tableName.compare("Columns")==0){
            //getTablesAttributes(col_attribute);
            attribute_Table=col_attribute;
        }
        else{
            getAttributes(tableName, attribute_Table);
        }

        int returnVal = rbfm->openFile(tableName, fileHandle);
        if(returnVal == -1)
            return -1;
        RBFM_ScanIterator rmsi;
        rbfm->scan(fileHandle, attribute_Table, conditionAttribute, compOp, value, attributeNames, rmsi);
        rm_ScanIterator.rbfmScanIterator=rmsi;
        rm_ScanIterator.fileHandle=fileHandle;
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        RC returnValue=rbfmScanIterator.getNextRecord(rid, data);
        return returnValue;
    }

    RC RM_ScanIterator::close() {
        rbfmScanIterator.close();
        rbfm->closeFile(fileHandle);
        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }
    /*
     * get the total length of attribute
     */
    int RelationManager::getAttributesLength(const vector<Attribute> &attr){
        int length = 0;
        for(const auto & i : attr){
            length += i.length;
            if(i.type == TypeVarChar){
                length += sizeof(int);
            }
        }
        return length;
    }
    /*
     * appending the buffer function (to make the code cleaner)
     */
    void RelationManager::append_buffer(const char* input, char* output, int input_length, const Attribute& attrs, int &current_offset){
        //if this is varchar then append the actual size
        if(attrs.type == TypeVarChar)
        {
            memcpy((char*)output + current_offset, &input_length, sizeof(int));
            current_offset += sizeof(int);
        }
        memcpy((char*)output + current_offset, input, input_length);
        current_offset += input_length;
    }
    /*
     * use to insert attributes of the table into table table
     */
    RC RelationManager::insertAttributeTable(int table_id, const string& tableName, const string& fileName, FileHandle &fileHandle, RID &rid){
        int attributeLength=getAttributesLength(table_attribute);
        char* data = new char[attributeLength];
        int offset = 0;
        char nullIndication = 0;
        Attribute attr_null;
        attr_null.type = TypeInt; // just for nullIndicator to not think it is VarChar
        //putting nullIndication in
        append_buffer(&nullIndication, data, sizeof(char), attr_null, offset);
        //putting table id in
        append_buffer((char*)&table_id, data, sizeof(int), table_attribute[0], offset);
        //putting table name in
        append_buffer(tableName.c_str(), data, tableName.length(), table_attribute[1], offset);
        //putting file name in
        append_buffer(fileName.c_str(), data, fileName.length(), table_attribute[2], offset);

        return rbfm->insertRecord(fileHandle, table_attribute, data, rid);
    }
    /*
     * use to insert attribute of the table into col table
     */
    RC RelationManager::insertAttributeCol(int table_id, const Attribute& attrs, FileHandle &fileHandle, int colPos, RID &rid){
        int attributeLength=getAttributesLength(col_attribute);
        char* data = new char[attributeLength];
        int offset = 0;
        char nullIndication = 0;
        Attribute attr_null;
        attr_null.type = TypeInt; // just for nullIndicator to not think it is VarChar
        //putting nullIndication in
        append_buffer(&nullIndication, data, sizeof(char), attr_null, offset);
        //putting table id in
        append_buffer((char*)&table_id, data, sizeof(int), col_attribute[0], offset);
        //putting col name in
        append_buffer(attrs.name.c_str(), data, attrs.name.length(), col_attribute[1], offset);
        //putting col type in
        append_buffer((char*)&attrs.type, data, sizeof(int), col_attribute[2], offset);
        //putting col length in
        append_buffer((char*)&attrs.length, data, sizeof(int), col_attribute[3], offset);
        //putting col position in
        append_buffer((char*)&colPos, data, sizeof(int), col_attribute[4], offset);

        return rbfm->insertRecord(fileHandle, col_attribute, data, rid);
    }
    /*
    * use to insert information of the index into index catalog table
    */
    RC RelationManager::insertAttributeIndex(const string& tableName, const string& attributeName, const string& indexName, FileHandle &fileHandle, RID &rid){
        int attributeLength=getAttributesLength(index_attribute);
        char* data = new char[attributeLength];
        int offset = 0;
        char nullIndication = 0;
        Attribute attr_null;
        attr_null.type = TypeInt; // just for nullIndicator to not think it is VarChar
        //putting nullIndication in
        append_buffer(&nullIndication, data, sizeof(char), attr_null, offset);
        //putting table id in
        append_buffer(indexName.c_str(), data, indexName.length(), index_attribute[0], offset);
        //putting table name in
        append_buffer(tableName.c_str(), data, tableName.length(), index_attribute[1], offset);
        //putting file name in
        append_buffer(attributeName.c_str(), data, attributeName.length(), index_attribute[2], offset);

        rbfm->insertRecord(fileHandle, index_attribute, data, rid);

    }

    /*
     * use to get attributes of Columns table
     */
    RC RelationManager::getColumnsAttributes(vector<Attribute>&columnsAttribute){
        columnsAttribute.clear();

        Attribute temp;
        temp.name = "table-id";
        temp.length = 4;
        temp.type = TypeInt;
        columnsAttribute.push_back(temp);

        temp.name = "column-name";
        temp.length = 50;
        temp.type = TypeVarChar;
        columnsAttribute.push_back(temp);

        temp.name = "column-type";
        temp.length = 4;
        temp.type = TypeInt;
        columnsAttribute.push_back(temp);

        temp.name = "column-length";
        temp.length = 4;
        temp.type = TypeInt;
        columnsAttribute.push_back(temp);

        temp.name = "column-position";
        temp.length = 4;
        temp.type = TypeInt;
        columnsAttribute.push_back(temp);
        return 0;
    }

    RC RelationManager::getTablesAttributes(vector<Attribute>&tablesAttribute){
        tablesAttribute.clear();

        Attribute temp;
        temp.name = "table-id";
        temp.length = 4;
        temp.type = TypeInt;
        tablesAttribute.push_back(temp);

        temp.name = "table-name";
        temp.length = 50;
        temp.type = TypeVarChar;
        tablesAttribute.push_back(temp);

        temp.name = "file-name";
        temp.length = 50;
        temp.type = TypeVarChar;
        tablesAttribute.push_back(temp);
        return 0;
    }

    RC RelationManager::getIndexAttributes(vector<Attribute>&indexAttribute){
        index_attribute.clear();
        Attribute temp;

        temp.name = "index-name";
        temp.length = 50;
        temp.type = TypeVarChar;
        index_attribute.push_back(temp);

        temp.name = "table-name";
        temp.length = 50;
        temp.type = TypeVarChar;
        index_attribute.push_back(temp);

        temp.name = "column-name";
        temp.length = 50;
        temp.type = TypeVarChar;
        index_attribute.push_back(temp);

        return 0;
    }

    // QE IX related

    RC RelationManager::createIndex(const string &tableName, const string &attributeName){
        FileHandle fileHandle;
        IXFileHandle ixFileHandle;
        RID rid;
        if(catalogStatus == 0){
            return -1;
        }
        string indexName = tableName + "_" + attributeName + ".idx";

        //PUT THIS TO PASS TEST FIX
        //ix->destroyFile(indexName);
        if(ix->createFile(indexName) != 0) {
            return -1;
        }

        //insert table-id:int, table-name:varchar(50), file-name:varchar(50) into table file
        if(rbfm->openFile(indexFileName, fileHandle) == -1){
            return -1;
        }
        if(insertAttributeIndex(tableName, attributeName, indexName, fileHandle, rid) == -1){
            return -1;
        }
        indexMap.insert({indexName, rid});

        if(rbfm->closeFile(fileHandle)){
            return -1;
        }
        vector<Attribute>attribute_table;
        vector<string> tableFetchAttributes;
        getAttributes(tableName, attribute_table);

        Attribute indexAttribute;

        for(int i=0;i<attribute_table.size();i++){
            if(attribute_table[i].name==attributeName){
                indexAttribute=attribute_table[i];
                tableFetchAttributes.push_back(attribute_table[i].name);
                break;
            }
        }
        //printf("Index name: %s, index attribute name: %s\n", indexName.c_str(), indexAttribute.name.c_str());
        RM_ScanIterator scanIterator;
        scan(tableName, attributeName, NO_OP, nullptr, tableFetchAttributes, scanIterator);
        char *outBuffer = new char[PAGE_SIZE];

        int count=0;
        vector<Attribute>printAttribute;
        printAttribute.push_back(indexAttribute);
        if(ix->openFile(indexName, ixFileHandle) != 0) {
            return -1;
        }
        while(scanIterator.getNextTuple(rid, outBuffer) != RM_EOF) {
            //printData(printAttribute, outBuffer);
            int record_size=tableFetchAttributes.size();
            int offset = ceil(record_size / 8.0);
            int isnull[record_size];
            for (int i = 0; i < offset; i++) {
                char checkNull;
                memcpy(&checkNull, (char *) outBuffer + i, 1);
                bitset<8> nullBitset(checkNull);
                for (int j = 0; j < 8 && 8 * i + j < record_size; j++) {
                    isnull[8 * i + j] = nullBitset.test(7 - j);
                }
            }
            if(isnull[0]==0){
                ix->insertEntry(ixFileHandle, indexAttribute, (char *)outBuffer+1, rid);

                count++;
            }
            //printf("count: %d\n", count);
            if(count==489){
                //printf("count: %d\n", count);
            }
        }
        ix->closeFile(ixFileHandle);
        //printf("Table name: %s, attribute name: %s, Total inserted entries: %d\n",tableName.c_str(), attributeName.c_str(), count);
        scanIterator.close();

        return 0;
    }

    RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
        FileHandle fileHandle;
        string indexName = tableName + "_" + attributeName + ".idx";
        //destroy the file of tableName
        if(ix->destroyFile(indexName) == -1){
            return -1;
        }
        //check if indexName exists in indexMap or not
        if(indexMap.find(indexName) != indexMap.end()){
            RID index_rid = indexMap[indexName];

            if(rbfm->openFile(indexFileName, fileHandle) == -1) {
                return -1;
            }

            if(rbfm->deleteRecord(fileHandle, index_attribute, index_rid) == -1){
                return -1;
            }
            indexMap.erase(indexName);
           return rbfm->closeFile(fileHandle);
        }
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                                  const std::string &attributeName,
                                  const void *lowKey,
                                  const void *highKey,
                                  bool lowKeyInclusive,
                                  bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator){
        vector<Attribute> attribute_Table;
        Attribute colAttribute;
        string indexName = tableName + "_" + attributeName + ".idx";

        //check if indexFile exist?
        if(ix->openFile(indexName, rm_IndexScanIterator.ixFileHandle) == -1){
            return -1;
        }
                //get Attribute
        if(getAttributes(tableName, attribute_Table) != 0)
            return -1;

        //check if this attributeName exists
        int check = 0;
        for(auto & i : attribute_Table){
            if(attributeName == i.name){
                check = 1;
                colAttribute=i;
                break;
            }
        }
        if(check == 0) {
            ix->closeFile(rm_IndexScanIterator.ixFileHandle);
            return -1;
        }

        //probably need to call ix scan?
        int returnVal=ix->scan(rm_IndexScanIterator.ixFileHandle, colAttribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ixScanIterator);

        return returnVal;
    }

    RC RelationManager:: printData(const std::vector<Attribute> &recordDescriptor, const void *data) {
        string recordString="";
        unsigned int record_size = recordDescriptor.size();
        int offset = ceil(record_size / 8.0);
        int isnull[record_size];
        for (int i = 0; i < offset; i++) {
            char checkNull;
            memcpy(&checkNull, (char *) data + i, 1);
            bitset<8> nullBitset(checkNull);
            for (int j = 0; j < 8 && 8 * i + j < record_size; j++) {
                isnull[8 * i + j] = nullBitset.test(7 - j);
            }
        }

        for (int i = 0; i < record_size; i++) {
            Attribute a = recordDescriptor[i];
            if (i != 0) {
                recordString += ", ";
            }
            recordString += a.name + ": ";
            if (isnull[i] != 0) {
                recordString += "NULL";
            } else {
                if (a.type == TypeInt) {
                    int intValue;
                    void *intStartPos = (char *) data + offset;
                    memcpy(&intValue, intStartPos, a.length);
                    offset += a.length;
                    recordString += to_string(intValue);
                } else if (a.type == TypeReal) {
                    float floatValue;
                    void *floatStartPos = (char *) data + offset;
                    memcpy(&floatValue, floatStartPos, a.length);
                    offset += a.length;
                    recordString += to_string(floatValue);
                } else if (a.type == TypeVarChar) {
                    int varcharActualLength;
                    void *varcharStartPos = (char *) data + offset;
                    memcpy(&varcharActualLength, varcharStartPos, sizeof(int));
                    char *charValue = new char[varcharActualLength];
                    memset(charValue, 0, varcharActualLength);
                    memcpy(charValue, (char *) varcharStartPos + sizeof(int), varcharActualLength);
                    string varcharValue;
                    for (int j = 0; j < varcharActualLength; j++) {
                        varcharValue += charValue[j];
                    }
                    offset += varcharActualLength + 4;
                    recordString += varcharValue;
                    // free(charValue);
                }
            }
        }
        printf("%s\n", recordString.c_str());
        return 0;
    }

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        return ixScanIterator.getNextEntry(rid, key);
    }

    RC RM_IndexScanIterator::close() {
        if (ixScanIterator.close() == -1) {
            ix->closeFile(ixFileHandle);
            return -1;
        }
        return ix->closeFile(ixFileHandle);
    }

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RM_IndexScanIterator::RM_IndexScanIterator() = default;
} // namespace PeterDB
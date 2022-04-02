#include "src/include/rbfm.h"
#include <ctgmath>
#include <cstring>
#include <cstdio>
#include <bits/stdc++.h>

using namespace std;

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    PagedFileManager *pfm = &PagedFileManager::instance();

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        //destroy before creating to make sure ?
        destroyFile(fileName);
        return pfm->createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {

        return pfm->destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        int returnValue = pfm->openFile(fileName, fileHandle);
        pageAvailability[fileHandle.fileName].clear();
        //if returnValue = 0 then continue to read pageAvailability otherwise return -1
        if (returnValue == 0) {
            int secondPartSize = PAGE_SIZE-fileHandle.pfmDataEndPos;
            char buffer[secondPartSize];

            fseek(fileHandle.F_pointer,fileHandle.pfmDataEndPos,SEEK_SET);
            fread(buffer, 1, secondPartSize, fileHandle.F_pointer);
            string actualValueStr(buffer, secondPartSize);
            if(fileHandle.numberOfHiddenPages>1){
                int hiddenPageIndex=0;
                int location=0;
                for(int i=1;i<fileHandle.numberOfHiddenPages-1;i++){
                    hiddenPageIndex = (i*(int)fileHandle.hiddenPageRate)+1;
                    location=hiddenPageIndex*PAGE_SIZE;

                    char bufferMultiple[PAGE_SIZE];

                    fseek(fileHandle.F_pointer,location,SEEK_SET);
                    fread(bufferMultiple, 1, PAGE_SIZE, fileHandle.F_pointer);
                    string bufferMultipleStr(bufferMultiple, PAGE_SIZE);
                    actualValueStr+=bufferMultipleStr;
                }
                hiddenPageIndex = ((fileHandle.numberOfHiddenPages-1)*fileHandle.hiddenPageRate)+1;;
                location=hiddenPageIndex*PAGE_SIZE;

                char bufferLast[PAGE_SIZE];

                fseek(fileHandle.F_pointer,location,SEEK_SET);
                fread(bufferLast, 1, PAGE_SIZE, fileHandle.F_pointer);
                string bufferLastStr(bufferLast, PAGE_SIZE);

                size_t padStart=bufferLastStr.find(" ");
                if(padStart != std::string::npos){
                    bufferLastStr = bufferLastStr.substr(0,padStart);
                }
                actualValueStr+=bufferLastStr;
            }
            vector<short> pageTemp;
            for (int i = 0; i < fileHandle.numberOfPages; i++) {
                size_t commaPos = actualValueStr.find(',');
                string readCount = actualValueStr.substr(0,(commaPos-0));
                actualValueStr = actualValueStr.substr(commaPos+1, actualValueStr.length());
                pageTemp.push_back(stoi(readCount));
            }
            pageAvailability[fileHandle.fileName]=pageTemp;
            //pageAvailability.insert({fileHandle.fileName, pageTemp});
            return 0;
        } else {
            return -1;
        }
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.F_pointer == NULL)
            return -1;

        int secondPartSize = PAGE_SIZE-fileHandle.pfmDataEndPos;

        string pfmData="";
        vector<short> pageTemp = pageAvailability[fileHandle.fileName];
        //put all pageAvailability into the hidden page
        for (int i = 0; i < fileHandle.numberOfPages; i++) {
            unsigned int a = pageTemp[i];
            pfmData += to_string(a) + ",";
        }
        if(pfmData.length()>secondPartSize){
            int offset=0;
            int hiddenPageIndex=0;
            int location=0;
            //Reset to default value
            fileHandle.numberOfHiddenPages=1;

            string firstSub=pfmData.substr(offset, secondPartSize);
            offset+=secondPartSize;
            string secondSub=pfmData.substr(offset, pfmData.length()-secondPartSize);
            //offset+=PAGE_SIZE;

            //save fir
            fseek(fileHandle.F_pointer, fileHandle.pfmDataEndPos, SEEK_SET);
            fwrite(firstSub.c_str(), 1, secondPartSize, fileHandle.F_pointer);

            while(secondSub.length()>PAGE_SIZE){
                firstSub=pfmData.substr(offset, PAGE_SIZE);
                offset+=PAGE_SIZE;
                secondSub=pfmData.substr(offset, pfmData.length()-PAGE_SIZE);


                hiddenPageIndex = (fileHandle.numberOfHiddenPages*fileHandle.hiddenPageRate)+1;
                location=hiddenPageIndex*PAGE_SIZE;

                fseek(fileHandle.F_pointer,location, SEEK_SET);
                fwrite(firstSub.c_str(), 1, PAGE_SIZE, fileHandle.F_pointer);
                fileHandle.numberOfHiddenPages+=1;
            }
            hiddenPageIndex = (fileHandle.numberOfHiddenPages*fileHandle.hiddenPageRate)+1;
            location=hiddenPageIndex*PAGE_SIZE;
            if(secondSub.length()<PAGE_SIZE){
                secondSub.append((PAGE_SIZE - secondSub.length()), ' ');
            }
            fseek(fileHandle.F_pointer,location, SEEK_SET);
            fwrite(secondSub.c_str(), 1, PAGE_SIZE, fileHandle.F_pointer);
            fileHandle.numberOfHiddenPages+=1;
        }
        else{
            //Reset to default
            fileHandle.numberOfHiddenPages=1;
            if(pfmData.length()<secondPartSize){
                pfmData.append((secondPartSize - pfmData.length()), ' ');
            }
            fseek(fileHandle.F_pointer,fileHandle.pfmDataEndPos, SEEK_SET);
            fwrite(pfmData.c_str(), 1, secondPartSize, fileHandle.F_pointer);
        }
        char debug[secondPartSize];

        fseek(fileHandle.F_pointer,fileHandle.pfmDataEndPos,SEEK_SET);
        fread(debug, 1, secondPartSize, fileHandle.F_pointer);
        string debugStr(debug, secondPartSize);
        return pfm->closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        //check if the fileHandle exists
        if (fileHandle.F_pointer == NULL) {
            return -1;
        }
        unsigned int recordSize = getLength(recordDescriptor, data);
        //printf("Table name: %s, recordSize %d \n", fileHandle.fileName.c_str(), recordSize);

        unsigned int numPage = 0;
        int newPage = -1;
        numPage = fileHandle.numberOfPages;
        vector<short> pageTemp = pageAvailability[fileHandle.fileName];
        //looping to find free space otherwise create a new page
        for (int i = 0; i < numPage; i++) {
            int availablePageSize = pageTemp[i];
            if (availablePageSize > recordSize + SLOT_SIZE) // 1 sizeof(int) for record, 1 sizeof(int) for pointer and 1 for tombstone counter
            {
                newPage = i;
                break;
            }
        }
        //if there is no free space in a page -> create new page
        if (newPage == -1) {
            //printf("create new page\n");
            //void *newPageData = malloc(PAGE_SIZE);
            char* newPageData = new char[PAGE_SIZE];
            memset(newPageData, 0, PAGE_SIZE);
            int num_zero = 0;
            int off_zero = 0;
            memcpy((char *) newPageData + NUM_RECORD, &num_zero, sizeof(int));
            memcpy((char *) newPageData + CUR_OFFSET, &off_zero , sizeof(int));
            fileHandle.appendPage(newPageData);
            pageTemp.push_back(PAGE_SIZE - 2 * sizeof(int));
            pageAvailability[fileHandle.fileName] = pageTemp;
            newPage = (int) fileHandle.numberOfPages - 1;
            //free(newPageData);
        }
        //add the record to the target Page
        //void *targetPage = malloc(PAGE_SIZE);
        char* targetPage = new char[PAGE_SIZE];
        memset(targetPage, 0, PAGE_SIZE);
        fileHandle.readPage(newPage, targetPage);
        int numberOfRecord;
        void *endOfPage = (char *) targetPage + NUM_RECORD; // get to data about number of records
        memcpy(&numberOfRecord, (char *) endOfPage, sizeof(int));
        //printf("numrecord check %d\n", numberOfRecord);
        endOfPage = (char *) targetPage + CUR_OFFSET; // get to the offset
        void *locationRecord; //location for putting our record
        void *locationPointer; //location for saving pointer and size of our new record
        int offset = 0; //offset to put it into the Page
        int checkEmpty = -1;
        for (int i = 0; i < numberOfRecord; i++) {
            int checkSlot;
            memcpy(&checkSlot, (char *) endOfPage - (i + 1) * (SLOT_SIZE) + 1, sizeof(int));
            if (checkSlot == -1) {
                checkEmpty = i;
                break;
            }
        }
        rid.pageNum = newPage;

        if (checkEmpty != -1) {
            rid.slotNum = checkEmpty;
            pageTemp[newPage] += SLOT_SIZE;
        }
            //assign pageNum and slotNum
        else {
            rid.slotNum = numberOfRecord;
        }

        if (numberOfRecord == 0) {
            locationRecord = (char *) targetPage;
            locationPointer = (char *) endOfPage - SLOT_SIZE;
            offset = 0;
        } else {
            //off to the location of data of previous record
            memcpy(&offset, (char *) endOfPage, sizeof(int));
            //printf("offset %d\n", offset);
            locationRecord = (char *) targetPage + offset;
            locationPointer = (char *) endOfPage - (rid.slotNum + 1) * SLOT_SIZE;
        }
        char *myRecord = new char[recordSize];
        memset(myRecord, 0, recordSize);
        //transform data into our format and put it into locationRecord
        encodeRecord(recordDescriptor, data, myRecord, recordSize);
        char tombstone = '0';
        // put tombstone and set to 0
        //printf("locationRecord: %d %d\n", recordSize, offset); //FIX
        memcpy((char *) locationPointer, &tombstone, sizeof(char));
        memcpy((char *) locationRecord, (char *) myRecord, recordSize);
        if (checkEmpty == -1)
            numberOfRecord++;
        // update our numberOfRecord in the page
        memcpy((char *) endOfPage + sizeof(int), &numberOfRecord, sizeof(int));
        //add the size of tombstone
        unsigned int newOffset = offset + recordSize;
        // update our latest offset Record in the page
        memcpy((char *) endOfPage, &newOffset, sizeof(int));
        //add recordSize into targetPage
        void *locationSize = (char *) locationPointer + sizeof(int) + 1;
        memcpy(locationSize, &recordSize, sizeof(int));
        // put the offset for the record
        memcpy((char *) locationPointer + 1, &offset, sizeof(int));
        //rewrite the page with the update version
        fileHandle.writePage(newPage, targetPage);
        // update the pageAvailability of the page with recordSize + size of pointer + size of data collecting the size of record
        pageTemp[newPage] -= (recordSize + SLOT_SIZE);
        pageAvailability[fileHandle.fileName] = pageTemp;
        //free(myRecord);
        //free(targetPage);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {

        //void *targetPage = malloc(PAGE_SIZE);
        char* targetPage = new char[PAGE_SIZE];
        memset(targetPage, 0, PAGE_SIZE);
        //printf("READ\n");
        RID newRid = rid;
        findRecord(fileHandle, recordDescriptor, targetPage, newRid);
        //printf("READ332 %d %d\n", newRid.pageNum, newRid.slotNum);
        unsigned pageNum = newRid.pageNum;
        unsigned short slotNum = newRid.slotNum;
        if (fileHandle.readPage(pageNum, targetPage) == 0) {
            //printf("READ2 %d %d\n", pageNum, slotNum);
            void *endOfPage = (char *) targetPage + PAGE_SIZE; // get to the end of page
            endOfPage = (char *) endOfPage - 2 * sizeof(int) - ((slotNum + 1) * (2 * sizeof(int) + 1));
            int lengthOfRecord;
            memcpy(&lengthOfRecord, (char *) endOfPage + 5, sizeof(int));
            char *record = new char[lengthOfRecord];
            memset(record, 0, lengthOfRecord);
            //if the record is deleted return -1 (fail)
            void *offset = (char *) endOfPage + 1;
            //printf("READ3 %d\n", *((int *) offset));
            if (*((int *) offset) == -1) {
                return -1;
            }
            void *recordLocation = (char *) targetPage + *((int *) offset);
            memcpy(record, (char *) recordLocation, lengthOfRecord);
            //need to transform record into the format given and send it to data
            decodeRecord(recordDescriptor, record, data, lengthOfRecord);
            //free(record);
            //free(targetPage);
            return 0;
        }
            //if readPage failed
        else {
            //free(targetPage);
            return -1;
        }
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        //check if the fileHandle exists
        if (fileHandle.F_pointer == NULL) {
            return -1;
        }
        //void *targetPage = malloc(PAGE_SIZE);
        char* targetPage = new char[PAGE_SIZE];
        //void *newPage = malloc(PAGE_SIZE);
        char* newPage = new char[PAGE_SIZE];
        memset(targetPage, 0, PAGE_SIZE);
        memset(newPage, 0, PAGE_SIZE);
        RID newRid = rid;
        if(find_deleteRecord(fileHandle, recordDescriptor, targetPage, newRid) == -1){
            return -1;
        }
        unsigned pageNum = newRid.pageNum;
        unsigned short slotNum = newRid.slotNum;
        if (fileHandle.readPage(pageNum, targetPage) == 0) {
            void *endOfPage = (char *) targetPage + PAGE_SIZE; // get to the end of page
            int numberOfSlot;
            memcpy(&numberOfSlot, (char *) endOfPage - 4, sizeof(int));//get total number of slot in this record
            endOfPage = (char *) endOfPage - 2 * sizeof(int) - ((slotNum + 1) * (2 * sizeof(int) + 1));
            int lengthOfRecord;
            memcpy(&lengthOfRecord, (char *) endOfPage + 5, sizeof(int));
            //need to move the record (and delete the record in the process)
            int offset = *(int *) ((char*)endOfPage + 1);
            moveRecord(targetPage, newPage, offset, lengthOfRecord, slotNum);
            //write the page with new adjusted page
            fileHandle.writePage(pageNum, newPage);
            vector<short> pageTemp = pageAvailability[fileHandle.fileName];
            pageTemp[pageNum] += lengthOfRecord;
            pageAvailability[fileHandle.fileName] = pageTemp;
            //free(targetPage);
            //free(newPage);
            return 0;
        }
            //if readPage failed
        else {
            //free(targetPage);
            //free(newPage);
            return -1;
        }
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        string recordString;
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
        out << recordString + "\n";
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        //check if the fileHandle exists
        if (fileHandle.F_pointer == NULL) {
            return -1;
        }
        //void *targetPage = malloc(PAGE_SIZE);
        char* targetPage = new char[PAGE_SIZE];
        memset(targetPage, 0, PAGE_SIZE);
        RID newRid = rid;
        int returnVal = fileHandle.readPage(rid.pageNum, targetPage);
        if (returnVal == -1) {
            //free(targetPage);
            return -1;
        }
/*
 *      //XD
        void *inputData = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, inputData);
        int numberOfRecord;
        int endPageOffset = PAGE_SIZE - sizeof(int);
        memcpy(&numberOfRecord, (char *) inputData + endPageOffset, sizeof(int));
        endPageOffset -= sizeof(int); // move pass the latestOffset counter
        int latestOffset = 0;
        for (int i = 0; i < numberOfRecord; i++) {
            endPageOffset -= (2 * sizeof(int) + 1);
            int current_offset;
            int current_length;
            char current_tomb;
            memcpy(&current_tomb, (char *) inputData + endPageOffset, sizeof(char));
            memcpy(&current_offset, (char *) inputData + endPageOffset + 1, sizeof(int));
            memcpy(&current_length, (char *) inputData + endPageOffset + sizeof(int) + 1, sizeof(int));
            if(current_tomb == '1') {
                printf("endofpageoffset: %d\t MOVE TO OTHER PAGE\n", endPageOffset);
                latestOffset += 9;
            }
            else {
                printf("endofpageoffset: %d\t current_offset: %d\t length: %d\t total: %d\n", endPageOffset,
                       current_offset, current_length, current_offset + current_length);
                latestOffset += 9 + current_length;
            }
        }
        printf("latestOffset: %d\n", PAGE_SIZE - latestOffset);
        free(inputData);
*/
        returnVal = findRecord(fileHandle, recordDescriptor, targetPage, newRid);
        if (returnVal == -1) {
            //free(targetPage);
            return -1;
        }
        returnVal = deleteRecord(fileHandle, recordDescriptor, newRid);
        if (returnVal == -1) {
            //free(targetPage);
            return -1;
        }
        RID temp;
        returnVal = insertRecord(fileHandle, recordDescriptor, data, temp);
        if (returnVal == -1) {
            //free(targetPage);
            return -1;
        }
        returnVal = fileHandle.readPage(newRid.pageNum, targetPage);
        vector<short> pageTemp = pageAvailability[fileHandle.fileName];
        //if it doesn't replace in the same slot but same Page, we need to change the slot to be correct value
        if (newRid.slotNum != temp.slotNum && newRid.pageNum == temp.pageNum && returnVal != -1) {
            //("NEWSLOT %d %d\n", newRid.slotNum, temp.slotNum);
            void *newRid_slot =
                    (char *) targetPage + PAGE_SIZE - 2 * sizeof(int) - ((newRid.slotNum + 1) * (2 * sizeof(int) + 1)) +
                    1;
            void *tempRid_slot =
                    (char *) targetPage + PAGE_SIZE - 2 * sizeof(int) - ((temp.slotNum + 1) * (2 * sizeof(int) + 1)) +
                    1;
            int tempOffset;
            int tempSize;
            int tempOffset_replace = -1;
            int tempSize_replace = 0;
            memcpy(&tempOffset, (char *) tempRid_slot, sizeof(int));
            memcpy(&tempSize, (char *) tempRid_slot + sizeof(int), sizeof(int));
            memcpy((char *) tempRid_slot, &tempOffset_replace, sizeof(int));
            memcpy((char *) tempRid_slot + sizeof(int), &tempSize_replace, sizeof(int));
            memcpy((char *) newRid_slot, &tempOffset, sizeof(int));
            memcpy((char *) newRid_slot + sizeof(int), &tempSize, sizeof(int));
            //pageTemp[newRid.pageNum] -= SLOT_SIZE;
            //pageAvailability[fileHandle.fileName] = pageTemp;
        }
            //else if it doesn't replace in the same slot and Page, we need to change give the new location and set tombstone to '1'
        else if (newRid.pageNum != temp.pageNum && returnVal != -1) {
            //printf("NEWPAGE %d %d\n", newRid.pageNum, temp.pageNum);
            void *newRid_tomb =
                    (char *) targetPage + (PAGE_SIZE - 2 * sizeof(int) - ((newRid.slotNum + 1) * (2 * sizeof(int) + 1)));
            char newTomb = '1';
            //change this data into location data
            memcpy(newRid_tomb, &newTomb, sizeof(char));
            int temp_offset = 0;
            auto s_pageNum = (short)temp.pageNum;
            auto s_slotNum = (short)temp.slotNum;
            memcpy((char *) newRid_tomb + 1, &temp_offset, sizeof(int));
            memcpy((char *) newRid_tomb + sizeof(int) + 1, &s_pageNum, sizeof(short));
            memcpy((char *) newRid_tomb + sizeof(int) + sizeof(short) + 1, &s_slotNum,  sizeof(short));
            //pageTemp[newRid.pageNum] -= SLOT_SIZE;
            //pageAvailability[fileHandle.fileName] = pageTemp;
        }
        if(returnVal == -1) {
            //free(targetPage);
            return -1;
        }
        fileHandle.writePage(newRid.pageNum, targetPage);
        //free(targetPage);
        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        unsigned pageNum = rid.pageNum;
        unsigned short slotNum = rid.slotNum;
        char *targetPage = new char[PAGE_SIZE];
        memset(targetPage, 0, PAGE_SIZE);
        if (fileHandle.readPage(pageNum, targetPage) == 0) {
            void *endOfPage = (char *) targetPage + PAGE_SIZE; // get to the end of page
            endOfPage = (char *) endOfPage - 2 * sizeof(int) - ((slotNum + 1) * (2 * sizeof(int) + 1));
            int lengthOfRecord;
            memcpy(&lengthOfRecord, (char *) endOfPage + 5, sizeof(int));
            char *record = new char[lengthOfRecord];
            memset(record, 0, lengthOfRecord);
            //if the record is deleted return -1 (fail)
            void *offset = (char *) endOfPage + 1;
            if (*((int *) offset) == -1) {
                return -1;
            }
            void *recordLocation = (char *) targetPage + *((int *) offset);
            char tombstone;
            memcpy(&tombstone, endOfPage, sizeof(char));
            if (tombstone != '0') {
                // need to call read record again to the new location may need to FIX
                RID newRid;
                short s_pageNum;
                short s_slotNum;
                memcpy(&s_pageNum, (char *) offset + sizeof(int), sizeof(short));
                memcpy(&s_slotNum, (char *) offset + sizeof(int) + sizeof(short), sizeof(short));
                newRid.pageNum = s_pageNum;
                newRid.slotNum = s_slotNum;
                //free(record);
                //free(targetPage);
                return readRecord(fileHandle, recordDescriptor, newRid, data);
            }
            memcpy(record, (char *) recordLocation, lengthOfRecord);
            //need to transform record into the format given and send it to data
            returnRecordAttribute(recordDescriptor, record, data, lengthOfRecord, attributeName);
            //free(record);
            //free(targetPage);
            return 0;
        }
            //if readPage failed
        else {
            //free(targetPage);
            return -1;
        }
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator.fileHandle=fileHandle;
        rbfm_ScanIterator.compOp=compOp;
        rbfm_ScanIterator.recordDescriptor=recordDescriptor;
        rbfm_ScanIterator.currentPos.pageNum=0;
        rbfm_ScanIterator.currentPos.slotNum=0;
        //rbfm_ScanIterator.currentPageData=malloc(PAGE_SIZE);
        memset(rbfm_ScanIterator.currentPageData, 0, PAGE_SIZE);

        for(int i=0;i<attributeNames.size();i++){
            string attributeName = attributeNames[i];
            for(int j=0;j<recordDescriptor.size();j++){
                Attribute a = recordDescriptor[j];
                if(attributeName.compare(a.name)==0){
                    rbfm_ScanIterator.attributeListSerial.push_back(j);
                    break;
                }
            }
        }
        if(compOp==NO_OP){
            return 0;
        }
        for(int j=0;j<recordDescriptor.size();j++){
            Attribute a=recordDescriptor[j];
            if(a.name.compare(conditionAttribute)==0){
                rbfm_ScanIterator.conditionAttributeSerial=j;
                rbfm_ScanIterator.conditionAttributeType=a.type;
                if(a.type==TypeInt){
                    rbfm_ScanIterator.conditionAttributeInt=*(const int *)value;
                }
                else if(a.type==TypeReal){
                    rbfm_ScanIterator.conditionAttributeFloat=*(const float *)value;
                }
                else{
                    string valueVarchar((char *)value);
                    rbfm_ScanIterator.conditionAttributeVarchar=valueVarchar;
                }
                break;
            }
        }
        return 0;
    }

    /*
     * This function uses to get the real length of record that we are going to keep
     * return the size of the record
     */
    unsigned int RecordBasedFileManager::getLength(const std::vector<Attribute> &recordDescriptor, const void *data) {
        unsigned int length = sizeof(int); // for storing the number of Attribute in recordDescriptor
        unsigned int current_pos = 0; //for storing the current position in data
        unsigned int record_size = recordDescriptor.size();
        //printf("recordDescriptor size: %d\n", record_size);
        length += record_size * sizeof(int); //for storing offset of record fields

        int offset = ceil(record_size / 8.0);
        current_pos += offset;
        int isnull[record_size];
        for (int i = 0; i < offset; i++) {
            char checkNull;
            memcpy(&checkNull, (char *) data + i, 1);
            bitset<8> nullBitset(checkNull);
            for (int j = 0; j < 8 && 8 * i + j < record_size; j++) {
                isnull[8 * i + j] = nullBitset.test(7 - j);
            }
        }

        //looping to get all data size from recordDescriptor and *data into the record
        for (int i = 0; i < record_size; i++) {
            length += sizeof(int);//for saving size of this attribute

            //if it is 0 then it is not null if it is 1 then it is null
            if (isnull[i] == 0) {
                Attribute a = recordDescriptor[i];
                //we don't need to do anything special for Int and Real number
                if (a.type == TypeInt || a.type == TypeReal) {
                    length += a.length;
                    current_pos += a.length;
                }
                    //we need to store as big of the size it needs
                else if (a.type == TypeVarChar) {
                    int varcharActualLength;
                    void *varcharStartPos = (char *) data + current_pos; // get to data about number of records
                    memcpy(&varcharActualLength, varcharStartPos, sizeof(int));
                    current_pos += sizeof(int);
                    length += varcharActualLength;
                    current_pos += varcharActualLength;
                }
            }
        }
        return length;
    }

    /*
     * This function uses to encode the given record into our format
     * return the size of the record
     */
    RC RecordBasedFileManager::encodeRecord(const std::vector<Attribute> &recordDescriptor, const void *inputData,
                                            void *outputData, unsigned int length) {
        int record_size = recordDescriptor.size();
        int inputOffset = 0;
        int outputOffset = 0;
        int pointerOffset = 0;

        //skip null indicator in the beginning
        inputOffset += ceil(record_size / 8.0);

        //store size of the record
        memcpy((char *) outputData + outputOffset, &length, sizeof(int));
        //skip size stored at the beginning
        outputOffset += sizeof(int);
        pointerOffset += sizeof(int);
        //skip offsets of fields stored after size
        outputOffset += 4 * record_size;

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
            int size = 0;

            Attribute a = recordDescriptor[i];

            if (isnull[i] != 0) {
                size = -1;
                //keep -1 in pointer if record is null << dont do this, check size is enough so just offset to the size
                memcpy((char *) outputData + pointerOffset, &outputOffset, sizeof(int));
                pointerOffset += sizeof(int);
                //keep -1 in size if record is null
                memcpy((char *) outputData + outputOffset, &size, sizeof(int));
                outputOffset += sizeof(int);
            } else {
                //Storing output offset in pointer
                memcpy((char *) outputData + pointerOffset, &outputOffset, sizeof(int));
                pointerOffset += sizeof(int);
                if (a.type == TypeInt || a.type == TypeReal) {
                    // size is equal to attribute length
                    size = a.length;
                } else if (a.type == TypeVarChar) {
                    //get actual length of varchar
                    memcpy(&size, (char *) inputData + inputOffset, sizeof(int));
                    //increase their offset to get varchar value
                    inputOffset += sizeof(int);
                }
                //storing size
                memcpy((char *) outputData + outputOffset, &size, sizeof(int));
                outputOffset += sizeof(int);
                //storing value;
                memcpy((char *) outputData + outputOffset, (char *) inputData + inputOffset, size);
                outputOffset += size;
                inputOffset += size;
            }
        }
        return 0;
    }

    /*
     * This function uses to decode our record back to the format given
     * return 0 if success -1 otherwise
     */
    RC RecordBasedFileManager::decodeRecord(const std::vector<Attribute> &recordDescriptor, const void *inputData,
                                            void *outputData, unsigned int length) {
        int record_size = recordDescriptor.size();
        int inputOffset = 0;
        int outputOffset = 0;

        int nullIndicationSize = ceil(record_size / 8.0);

        //to skip null indicator and directly go to values
        outputOffset += nullIndicationSize;
        //to skip record size at the beginning
        inputOffset += sizeof(int);
        //to skip offset stored after size
        inputOffset += 4 * record_size;
        string isnullString = "";
        for (int i = 0; i < record_size; i++) {
            Attribute a = recordDescriptor[i];
            int size;
            memcpy(&size, (char *) inputData + inputOffset, sizeof(int));
            inputOffset += sizeof(int);
            if (size == -1) {
                //indicates null field in our format
                // set 1 for the field in null indicator
                isnullString.append("1");
            } else {
                isnullString.append("0");
                if (a.type == TypeVarChar) {
                    //store actual length of varchar
                    memcpy((char *) outputData + outputOffset, &size, sizeof(int));
                    outputOffset += sizeof(int);
                }
                //storing value for all types: int, real, varchar
                memcpy((char *) outputData + outputOffset, (char *) inputData + inputOffset, size);
                inputOffset += size;
                outputOffset += size;
            }
        }
        auto indicator = new unsigned char[nullIndicationSize];
        for (int i = 0; i < nullIndicationSize; i++) {
            unsigned int isnull = 0;
            for (int j = 0; j < 8 && i * 8 + j < record_size; j++) {
                if (isnullString.at(i * 8 + j) == '1') {
                    isnull += 1;
                }
                isnull = isnull << (unsigned) 1;
            }
            if ((i + 1) * 8 > record_size)
                isnull = isnull << (unsigned) (8 * (i + 1) - record_size - 1);

            indicator[i] = isnull;
        }
        memcpy((char *) outputData, indicator, nullIndicationSize);
        return 0;
    }

    RC RecordBasedFileManager::returnRecordAttribute(const std::vector<Attribute> &recordDescriptor, const void *inputData,
                                                     void *outputData, unsigned int length, const std::string &attributeName) {
        int record_size = recordDescriptor.size();
        int inputOffset = 0;
        int outputOffset=0;
        int attributeSerial=0;

        int nullIndicationSize = 1;

        for (int i = 0; i < record_size; i++){
            Attribute a = recordDescriptor[i];
            if(a.name.compare(attributeName)==0){
                attributeSerial=i;
                break;
            }
        }

        //to skip record size at the beginning
        inputOffset += sizeof(int);
        //to skip previous offsets stored after size
        inputOffset += 4 * attributeSerial;

        int attributeOffset;
        memcpy(&attributeOffset, (char *) inputData + inputOffset, sizeof(int));

        inputOffset = attributeOffset;

        string isnullString = "";

        int size;
        memcpy(&size, (char *) inputData + inputOffset, sizeof(int));
        inputOffset += sizeof(int);
        int outputLength=0;
        //store nullIndicator at the beginning
        outputLength+=nullIndicationSize;

        if(recordDescriptor[attributeSerial].type==TypeVarChar){
            //store size for varchar
            outputLength+=sizeof(int);
        }

        if(size!=-1){
            //if not null then store value
            outputLength+=size;
        }
        //skip 1 byte null indicator at the start
        outputOffset+=1;
        if(size!=-1){
            isnullString.append("0");

            if(recordDescriptor[attributeSerial].type==TypeVarChar){
                memcpy((char *) outputData+outputOffset, &size, sizeof(int));
                outputOffset += sizeof(int);
            }

            //storing value;
            memcpy((char *) outputData + outputOffset, (char *) inputData + inputOffset, size);
        }
        else{
            isnullString.append("1");
        }

        auto indicator = new unsigned char[nullIndicationSize];
        for (int i = 0; i < nullIndicationSize; i++) {
            unsigned int isnull = 0;
            for (int j = 0; j < 8 && i * 8 + j < 1; j++) {
                if (isnullString.at(i * 8 + j) == '1') {
                    isnull += 1;
                }
                isnull = isnull << (unsigned) 1;
            }
            if ((i + 1) * 8 > 1)
                isnull = isnull << (unsigned) (8 * (i + 1) - 1 - 1);

            indicator[i] = isnull;
        }
        memcpy((char *) outputData, indicator, nullIndicationSize);
        return 0;
    }

    /*
     * This function uses to move our record after deleted/update the record
     * return 0 if success -1 otherwise
     */
    RC RecordBasedFileManager::moveRecord(const void *inputData, void *outputData, int offset, int length, int slotNum) {
        if (inputData == nullptr)
            return -1;
        //deal with the end of the file
        int numberOfRecord;
        int endPageOffset = PAGE_SIZE - sizeof(int);
        memcpy(&numberOfRecord, (char *) inputData + endPageOffset, sizeof(int));
        memcpy((char *) outputData + endPageOffset, &numberOfRecord, sizeof(int));
        endPageOffset -= sizeof(int); // move pass the latestOffset counter
        //for the deleted record
        int newOffset = -1;
        int newSize = 0;
        char newTomb = '1';
        int latestOffset = 0;
        //printf("OFFSET: %d\n", offset);
        for (int i = 0; i < numberOfRecord; i++) {
            endPageOffset -= (2 * sizeof(int) + 1);
            if (i == slotNum) {
                memcpy((char *) outputData + endPageOffset, &newTomb, sizeof(char));
                memcpy((char *) outputData + endPageOffset + 1, &newOffset, sizeof(int));
                memcpy((char *) outputData + endPageOffset + sizeof(int) + 1, &newSize, sizeof(int));
            } else {
                int current_offset;
                int current_length;
                char current_tomb;
                memcpy(&current_tomb, (char *) inputData + endPageOffset, sizeof(char));
                memcpy(&current_offset, (char *) inputData + endPageOffset + 1, sizeof(int));
                memcpy(&current_length, (char *) inputData + endPageOffset + sizeof(int) + 1, sizeof(int));
                //printf("endofpageoffset: %d\t current_offset: %d\t length: %d\t total: %d  ||| %d vs %d\n", endPageOffset, current_offset, current_length, current_offset+current_length, i, slotNum);
                if (current_offset == -1 || current_length == -1) { //meaning it is deleted slot or null
                    memcpy((char *) outputData + endPageOffset, &current_tomb, sizeof(char));
                    memcpy((char *) outputData + endPageOffset + 1, &current_offset, sizeof(int));
                    memcpy((char *) outputData + endPageOffset + sizeof(int) + 1, &current_length, sizeof(int));
                    if (latestOffset < current_offset)
                        latestOffset = current_offset;
                }
                else if (current_tomb == '1'){//doesn't have data outside the slot
                    memcpy((char *) outputData + endPageOffset, &current_tomb, sizeof(char));
                    memcpy((char *) outputData + endPageOffset + 1, &current_offset, sizeof(int));
                    memcpy((char *) outputData + endPageOffset + sizeof(int) + 1, &current_length, sizeof(int));
                }
                else if (current_offset < offset) { //don't need to move data
                    memcpy((char *) outputData + endPageOffset, &current_tomb, sizeof(char));
                    memcpy((char *) outputData + current_offset, (char *) inputData + current_offset, current_length);
                    memcpy((char *) outputData + endPageOffset + 1, &current_offset, sizeof(int));
                    memcpy((char *) outputData + endPageOffset + sizeof(int) + 1, &current_length, sizeof(int));
                    if (latestOffset < current_offset + current_length)
                        latestOffset = current_offset + current_length;
                } else { // need to move for compact
                    memcpy((char *) outputData + endPageOffset, &current_tomb, sizeof(char));
                    memcpy((char *) outputData + current_offset - length, (char *) inputData + current_offset,
                           current_length);
                    current_offset -= length;
                    memcpy((char *) outputData + endPageOffset + 1, &current_offset, sizeof(int));
                    memcpy((char *) outputData + endPageOffset + sizeof(int) + 1, &current_length, sizeof(int));
                    if (latestOffset < current_offset + current_length)
                        latestOffset = current_offset + current_length;
                }
            }
        }
        memcpy((char *) outputData + PAGE_SIZE - 2 * sizeof(int), &latestOffset, sizeof(int));
        return 0;
    }

    /*
     * This function uses to find the real location of the record
     * return 0 if success -1 otherwise
     */
    RC RecordBasedFileManager::findRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          void *targetPage, RID &newRid) {
        int pageNum = (int)newRid.pageNum;
        int slotNum = (int)newRid.slotNum;
        if (fileHandle.readPage(pageNum, targetPage) == 0) {
            void *endOfPage = (char *) targetPage + PAGE_SIZE; // get to the end of page
            endOfPage = (char *) endOfPage - 2 * sizeof(int) - ((slotNum + 1) * (2 * sizeof(int) + 1));
            int lengthOfRecord;
            memcpy(&lengthOfRecord, (char *) endOfPage + 5, sizeof(int));
            void *offset = (char *) endOfPage + 1;
            //if the record is deleted return -1 (fail)
            if (*((int *) offset) == -1) {
                return -1;
            }
            char tombstone;
            memcpy(&tombstone, endOfPage, sizeof(char));
            if (tombstone != '0') {
                short s_pageNum;
                short s_slotNum;
                memcpy(&s_pageNum, (char *) offset + sizeof(int), sizeof(short));
                memcpy(&s_slotNum, (char *) offset + sizeof(int) + sizeof(short), sizeof(short));
                newRid.pageNum = s_pageNum;
                newRid.slotNum = s_slotNum;
                return findRecord(fileHandle, recordDescriptor, targetPage, newRid);
            }
            return 0;
        } else
            return -1;
    }

    RC RecordBasedFileManager::find_deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void *targetPage, RID &newRid) {
        int pageNum = (int)newRid.pageNum;
        int slotNum = (int)newRid.slotNum;
        if (fileHandle.readPage(pageNum, targetPage) == 0) {
            void *endOfPage = (char *) targetPage + PAGE_SIZE; // get to the end of page
            endOfPage = (char *) endOfPage - 2 * sizeof(int) - ((slotNum + 1) * (2 * sizeof(int) + 1));
            int lengthOfRecord;
            memcpy(&lengthOfRecord, (char *) endOfPage + 5, sizeof(int));
            void *offset = (char *) endOfPage + 1;
            //if the record is deleted return -1 (fail)
            if (*((int *) offset) == -1) {
                return -1;
            }
            char tombstone;
            memcpy(&tombstone, endOfPage, sizeof(char));
            if (tombstone != '0') {
                short s_pageNum;
                short s_slotNum;
                memcpy(&s_pageNum, (char *) offset + sizeof(int), sizeof(short));
                memcpy(&s_slotNum, (char *) offset + sizeof(int) + sizeof(short), sizeof(short));
                newRid.pageNum = s_pageNum;
                newRid.slotNum = s_slotNum;
                //delete every slot along the  way
                tombstone = '0';
                //pageAvailability[pageNum] += SLOT_SIZE;
                memcpy((char *)endOfPage, &tombstone, sizeof(char));
                int newOffset = -1;
                memcpy((char *)endOfPage + 1, &newOffset, sizeof(int));
                int newLength = 0;
                memcpy((char *)endOfPage + 1 + sizeof(int), &newLength, sizeof(int));
                fileHandle.writePage(pageNum, targetPage);
                return find_deleteRecord(fileHandle, recordDescriptor, targetPage, newRid);
            }
            return 0;
        } else
            return -1;
    }
} // namespace PeterDB
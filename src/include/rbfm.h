#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>
#include <cstring>
#include <ctgmath>
#include <map>

#include "pfm.h"
using namespace std;
namespace PeterDB {
    // Record ID
    typedef struct {
        unsigned pageNum;           // page number
        unsigned short slotNum;     // slot number in the page
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;

    const size_t NUM_RECORD = PAGE_SIZE - sizeof(int);
    const size_t CUR_OFFSET = PAGE_SIZE - 2*sizeof(int);
    const size_t SLOT_SIZE = 2*sizeof(int) + 1;

    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();

    class RBFM_ScanIterator {
    public:
        FileHandle fileHandle;
        vector<Attribute> recordDescriptor;
        int conditionAttributeSerial;
        CompOp compOp;
        AttrType conditionAttributeType;
        string conditionAttributeVarchar;
        int conditionAttributeInt;
        float conditionAttributeFloat;
        vector<int>attributeListSerial;

        RID currentPos;
        int currentPageNumOfRecords;
        char *currentPageData=new char[PAGE_SIZE];

        RBFM_ScanIterator() = default;;

        ~RBFM_ScanIterator() = default;;

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data) {
            while(currentPos.pageNum<fileHandle.numberOfPages){
                if(currentPos.slotNum==0){
                    //Instead of reading each record from disk, read full page and keep in memory
                    if (fileHandle.readPage(currentPos.pageNum, currentPageData) != 0){
                        return RBFM_EOF;
                    }
                    void *numberOfRecordsLocation = (char *) currentPageData + PAGE_SIZE - sizeof(int); // get to data about number of records
                    memcpy(&currentPageNumOfRecords, (char *) numberOfRecordsLocation, sizeof(int));
                }
                void *endOfPage = (char *) currentPageData + PAGE_SIZE;

                endOfPage = (char *) endOfPage - 2 * sizeof(int) - ((currentPos.slotNum + 1) * (2 * sizeof(int) + 1));


                void *offset = (char *) endOfPage + 1;
                //If the record is deleted continue to next record
                if (*((int *) offset) != -1) {
                    char tombstone;
                    memcpy(&tombstone, endOfPage, sizeof(char));
                    //If it is a record pointer, continue to next record
                    if (tombstone == '0') {
                        bool fulfilCondition=false;
                        //read current record from page
                        void *recordLocation = (char *) currentPageData + *((int *) offset);
                        int lengthOfRecord;
                        memcpy(&lengthOfRecord, (char *) endOfPage + 5, sizeof(int));
                        char *record = new char[lengthOfRecord];
                        memcpy(record, (char *) recordLocation, lengthOfRecord);

                        if(compOp==NO_OP){
                            fulfilCondition=true;
                        }
                        else{
                            //get condition attribute value
                            int inputOffset = 0;
                            //to skip record size at the beginning
                            inputOffset += sizeof(int);
                            //to skip previous offsets stored after size
                            inputOffset += 4 * conditionAttributeSerial;

                            //get attribute offset
                            int attributeOffset;
                            memcpy(&attributeOffset, (char *) record + inputOffset, sizeof(int));

                            inputOffset = attributeOffset;

                            //read size of condition attribute
                            int size;
                            memcpy(&size, (char *) record + inputOffset, sizeof(int));
                            inputOffset += sizeof(int);

                            //If condition attribute value is null, continue to next record
                            if(size!=-1){
                                //read value of condition attribute
                                char * conditionalValue = new char[size];
                                memcpy((char *) conditionalValue, (char *) record + inputOffset, size);

                                //check condition value based on attribute type, compare operator and provided value

                                if(conditionAttributeType==TypeInt){
                                    int conditionalValueInt=*(const int *)conditionalValue;
                                    switch (compOp) {
                                        case EQ_OP:
                                            if(conditionalValueInt==conditionAttributeInt){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case GT_OP:
                                            if(conditionalValueInt>conditionAttributeInt){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case GE_OP:
                                            if(conditionalValueInt>=conditionAttributeInt){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case LT_OP:
                                            if(conditionalValueInt<conditionAttributeInt){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case LE_OP:
                                            if(conditionalValueInt<=conditionAttributeInt){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case NE_OP:
                                            if(conditionalValueInt!=conditionAttributeInt){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case NO_OP:
                                            fulfilCondition= true;
                                            break;
                                    }
                                }
                                else if(conditionAttributeType==TypeReal){
                                    float conditionalValueFloat=*(const float *)conditionalValue;
                                    switch (compOp) {
                                        case EQ_OP:
                                            if(conditionalValueFloat==conditionAttributeFloat){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case GT_OP:
                                            if(conditionalValueFloat>conditionAttributeFloat){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case GE_OP:
                                            if(conditionalValueFloat>=conditionAttributeFloat){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case LT_OP:
                                            if(conditionalValueFloat<conditionAttributeFloat){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case LE_OP:
                                            if(conditionalValueFloat<=conditionAttributeFloat){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case NE_OP:
                                            if(conditionalValueFloat!=conditionAttributeFloat){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case NO_OP:
                                            fulfilCondition= true;
                                            break;
                                    }
                                }
                                else{
                                    string conditionalValueVarchar((char *)conditionalValue);
                                    if(conditionalValueVarchar.length()>size){
                                        conditionalValueVarchar=conditionalValueVarchar.substr(0,size);
                                    }
                                    switch (compOp) {
                                        case EQ_OP:
                                            if(conditionalValueVarchar.compare(conditionAttributeVarchar)==0){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case GT_OP:
                                            if(conditionalValueVarchar.compare(conditionAttributeVarchar)>0){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case GE_OP:
                                            if(conditionalValueVarchar.compare(conditionAttributeVarchar)>=0){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case LT_OP:
                                            if(conditionalValueVarchar.compare(conditionAttributeVarchar)<0){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case LE_OP:
                                            if(conditionalValueVarchar.compare(conditionAttributeVarchar)<=0){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case NE_OP:
                                            if(conditionalValueVarchar!=conditionAttributeVarchar){
                                                fulfilCondition= true;
                                            }
                                            break;
                                        case NO_OP:
                                            fulfilCondition= true;
                                            break;
                                    }
                                }
                            }
                        }
                        //if condition attribute value does not satisfy the condition, continue to next record
                        if(fulfilCondition){
                            //assign current position to rid
                            rid.pageNum=currentPos.pageNum;
                            rid.slotNum=currentPos.slotNum;
                            //get filtered record length
                            int recordLength = getSelectedFieldsLength(record, recordDescriptor, attributeListSerial);

                            //construct filtered record
                            //data=malloc(recordLength);

                            string isnullString="";
                            int recordSize=attributeListSerial.size();
                            int nullIndicatorSize=ceil(recordSize/8.0);

                            int outputOffset=0;
                            //Skip null indicator at the beginning
                            outputOffset+=nullIndicatorSize;

                            //store values of selected fields
                            for(int i=0;i<recordSize;i++){
                                int fieldSerial = attributeListSerial[i];
                                Attribute a=recordDescriptor[fieldSerial];
                                int temp_inputOffset=0;

                                //to skip record size at the beginning
                                temp_inputOffset += sizeof(int);
                                //to skip previous offsets stored after size
                                temp_inputOffset += 4 * fieldSerial;

                                //get attribute offset
                                int temp_attributeOffset;
                                memcpy(&temp_attributeOffset, (char *) record + temp_inputOffset, sizeof(int));

                                temp_inputOffset =  temp_attributeOffset;

                                //get size of attribute
                                int size;
                                memcpy(&size, (char *) record + temp_inputOffset, sizeof(int));
                                temp_inputOffset+=sizeof(int);
                                if(size==-1){
                                    isnullString.append("1");
                                }
                                else{
                                    isnullString.append("0");

                                    if(a.type==TypeVarChar){
                                        //store size for varchar
                                        memcpy((char *) data+outputOffset, &size, sizeof(int));
                                        outputOffset += sizeof(int);
                                    }

                                    //storing value;
                                    memcpy((char *) data + outputOffset, (char *) record + temp_inputOffset, size);
                                    /*int tableId;
                                    memcpy(&tableId, (char*)data + outputOffset, sizeof(int));
                                    int tableIDReal;
                                    memcpy(&tableIDReal, (char *) record + temp_inputOffset, size);*/
                                    outputOffset+=size;
                                }
                            }
                            //get null indicator
                            auto indicator = new unsigned char[nullIndicatorSize];
                            for (int i = 0; i < nullIndicatorSize; i++) {
                                unsigned int isnull = 0;
                                for (int j = 0; j < 8 && i * 8 + j < recordSize; j++) {
                                    if (isnullString.at(i * 8 + j) == '1') {
                                        isnull += 1;
                                    }
                                    isnull = isnull << (unsigned) 1;
                                }
                                if ((i + 1) * 8 > recordSize)
                                    isnull = isnull << (unsigned) (8 * (i + 1) - recordSize - 1);

                                indicator[i] = isnull;
                            }
                            //store null indicator at the beginning of filtered record
                            memcpy((char *) data, indicator, nullIndicatorSize);

                            //update current position of cursor
                            currentPos.slotNum+=1;
                            if(currentPos.slotNum>=currentPageNumOfRecords){
                                currentPos.pageNum+=1;
                                currentPos.slotNum=0;
                            }
                            return 0;
                        }
                    }
                }
                //update current position of cursor
                currentPos.slotNum+=1;
                if(currentPos.slotNum>=currentPageNumOfRecords){
                    currentPos.pageNum+=1;
                    currentPos.slotNum=0;
                }
            }
            return RBFM_EOF;
        };

        RC close() {
            currentPos.pageNum=0;
            currentPos.slotNum=0;
            //free(currentPageData);
            return 0;
        };

        RC getSelectedFieldsLength(void* record, vector<Attribute> reDescriptor, vector<int>fieldListSerial){
            int length=0;
            int recordSize=fieldListSerial.size();
            int nullIndicatorSize = ceil(recordSize / 8.0);
            length+=nullIndicatorSize;

            for(int i=0;i<recordSize;i++){
                int fieldSerial = fieldListSerial[i];
                Attribute a=reDescriptor[fieldSerial];
                int inputOffset=0;
                //to skip record size at the beginning
                inputOffset += sizeof(int);
                //to skip previous offsets stored after size
                inputOffset += 4 * fieldSerial;

                int attributeOffset;
                memcpy(&attributeOffset, (char *) record + inputOffset, sizeof(int));

                inputOffset = attributeOffset;

                int size;
                memcpy(&size, (char *) record + inputOffset, sizeof(int));
                if(size!=-1){
                    length+=size;
                }
            }
            return length;
        }
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        unsigned int getLength(const std::vector<Attribute> &recordDescriptor, const void *data);

        RC encodeRecord(const std::vector<Attribute> &recordDescriptor, const void* inputData, void* outputData, unsigned int length);

        RC decodeRecord(const std::vector<Attribute> &recordDescriptor, const void* inputData, void* outputData,  unsigned int length);

        RC moveRecord(const void* inputData, void* outputData, int offset, int length, int slotNum);

        std::map<std::string, vector<short>> pageAvailability;

        RC findRecord (FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void* targetPage, RID &newRid);

        RC find_deleteRecord (FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void* targetPage, RID &newRid);
        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        RC returnRecordAttribute(const std::vector<Attribute> &recordDescriptor, const void *inputData, void *outputData,
                                 unsigned int length, const std::string &attributeName);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

    };

} // namespace PeterDB

#endif // _rbfm_h_
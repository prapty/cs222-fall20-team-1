#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "src/include/rbfm.h"
#include "ix.h"
#include<map>

using namespace std;

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator
#define INSERT_INDEX_ENTRY (1)
#define DELETE_INDEX_ENTRY (2)
    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        FileHandle fileHandle;
        RBFM_ScanIterator rbfmScanIterator;
        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        IXFileHandle ixFileHandle;
        IX_ScanIterator ixScanIterator;
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        RC insertAttributeTable(int table_id, const std::string& tableName, const std::string& fileName, FileHandle &fileHandle, RID &rid);

        RC insertAttributeCol(int table_id, const Attribute& attrs,  FileHandle &fileHandle, int colPos, RID &rid);

        RC insertAttributeIndex(const string& tableName, const string& attributeName, const string& indexName, FileHandle &fileHandle, RID &rid);

        RC getTableId(const std::string &tableName);

        RC getColumnDetails(vector<Attribute> &attrs, int tableId);

        FileHandle table_fileHandle;
        FileHandle col_fileHandle;

        vector<Attribute> table_attribute;
        vector<Attribute> col_attribute;
        vector<Attribute> index_attribute;

        int table_count = 0;

        string tableFileName = "Tables";
        string colFileName = "Columns";
        string indexFileName = "Index_team_1";

        int TABLE_ID = 1;
        int catalogStatus = 0;

        map<string, RID> tabMap;
        map<string, map<int, RID>> colMap;
        map<string, RID> indexMap;
        map<string , vector<Attribute>> attributeMap;

        int getAttributesLength(const std::vector<Attribute> &attr);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

        RC printData(const std::vector<Attribute> &recordDescriptor, const void *data);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

        void
        append_buffer(const char *input, char *output, int input_length, const Attribute &attrs, int &current_offset);

        RC getColumnsAttributes(vector<Attribute> &columnAttribute);

        RC getTablesAttributes(vector<Attribute> &columnAttribute);

        RC getIndexAttributes(vector<Attribute> &indexAttribute);

        RC indexOperations(int operation, const void *inputData, const vector<Attribute> &attrs, const std::string &tableName, const RID &rid);
    };

} // namespace PeterDB

#endif // _rm_h_
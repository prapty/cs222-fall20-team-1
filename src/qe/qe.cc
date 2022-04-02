#include "src/include/qe.h"
#include <algorithm>
#include <bits/stdc++.h>

namespace PeterDB {

    //Filter START
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->iterator = input;
        op = condition.op;
        this->iterator->getAttributes(attribute);
        this->condition = condition.rhsValue.data;
        conType = condition.rhsValue.type;
        for (int i = 0; i < attribute.size(); i++) {
            //find the condition attribute
            if (condition.lhsAttr == attribute[i].name) {
                position = i;
                this->data = new char[attribute[i].length + sizeof(int)];
                break;
            }
        }
    }

    Filter::~Filter() {
    }

    RC Filter::getNextTuple(void *data) {
        int check = 0;
        int countAll=0;
        while(check!=RM_EOF){
            countAll++;
            check = iterator->getNextTuple(data);
            if(check != 0) {
               // printf("Total entries: %d\n",countAll);
                return check;
            }

            getData(data, this->data, attribute, position, conType);

            if(compareData(this->data, condition, conType, op)==true) {
                break;
            }
        }
        return check;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attribute;
        return 0;
    }
    //Filter END

    //Project START
    //use to get selected column from the table
    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        attribute.clear();
        this->iterator = input;
        this->iterator->getAttributes(beforeAttribute);
        this->attributeNames=attrNames;
        this->data = new char[PAGE_SIZE];

        for(int i=0;i<attrNames.size();i++){
            for (int j=0;j<beforeAttribute.size();j++){
                if(attrNames[i]==beforeAttribute[j].name){
                    attribute.push_back(beforeAttribute[j]);
                    beforeAttributePositionMap.insert({beforeAttribute[j].name, j});
                    break;
                }
            }
        }
        if(attribute.size()==attrNames.size()){
            //printf("Attribute Populated correctly in new code.\n");
        }
    }

    Project::~Project() {
    }

    RC Project::getNextTuple(void *data) {
        if(!iterator->getNextTuple(this->data)){
            int inputRecordSize=beforeAttribute.size();
            int outputRecordSize=attribute.size();
            int inputOffset = ceil(inputRecordSize/8.0);
            int outputOffset = ceil(outputRecordSize/8.0);
            string isnullString = "";
            int offset = ceil(inputRecordSize / 8.0);
            //null indicator processing
            int isnull[inputRecordSize];
            for (int i = 0; i < offset; i++) {
                char checkNull;
                memcpy(&checkNull, (char *) this->data + i, 1);
                bitset<8> nullBitset(checkNull);
                for (int j = 0; j < 8 && 8 * i + j < inputRecordSize; j++) {
                    isnull[8 * i + j] = nullBitset.test(7 - j);
                }
            }
            for(int i=0;i<attribute.size();i++){
                Attribute a=attribute[i];
                int originalPosition=beforeAttributePositionMap[a.name];
                int length=0;
                if(isnull[i]==0){
                    if(a.type == TypeVarChar){
                        length = *(int*)((char*) this->data + inputOffset) + (int)sizeof(int);
                    }
                    else{
                        length = sizeof(int);
                    }
                    getData((char*)this->data, (char*)data + outputOffset, beforeAttribute, originalPosition, a.type);
                    isnullString.append("0");
                }
                else{
                    isnullString.append("1");
                }
                outputOffset += length;
            }
            //put null indicator in output data
            unsigned int nullIndicationSize=ceil(outputRecordSize/8.0);
            auto indicator = new unsigned char[nullIndicationSize];
            for (int i = 0; i < nullIndicationSize; i++) {
                unsigned int isnull = 0;
                for (int j = 0; j < 8 && i * 8 + j < outputRecordSize; j++) {
                    if (isnullString.at(i * 8 + j) == '1') {
                        isnull += 1;
                    }
                    isnull = isnull << (unsigned) 1;
                }
                if ((i + 1) * 8 > outputRecordSize)
                    isnull = isnull << (unsigned) (8 * (i + 1) - outputRecordSize - 1);

                indicator[i] = isnull;
            }
            memcpy((char *) data, indicator, nullIndicationSize);
            return 0;
        }
        return -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attribute;
        return 0;
    }
    //Project END

    //BNLJoin START
    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        iterator = leftIn;
        tableScan = rightIn;
        //get Attribute from leftIn and rightIn
        leftIn->getAttributes(leftAttribute);
        rightIn->getAttributes(rightAttribute);
        pageNum = numPages;
        op = condition.op;
        conType = condition.rhsValue.type;
        this->leftBlock = new char[PAGE_SIZE];
        this->rightBlock = new char[PAGE_SIZE];
        //get the position of condition attribute for leftIn
        for(int i = 0; i < leftAttribute.size(); i++){
            //get all attribute from left table
            attribute.push_back(leftAttribute[i]);
            if (condition.lhsAttr == leftAttribute[i].name){
                leftPos = i;
                this->leftData = new char[leftAttribute[i].length + sizeof(int)];
            }
        }
        //get the position of condition attribute for rightIn
        for(int i = 0; i < rightAttribute.size(); i++){
            //get all attribute from right table
            attribute.push_back(rightAttribute[i]);
            if (condition.rhsAttr == rightAttribute[i].name) {
                rightPos = i;
                this->rightData = new char[rightAttribute[i].length + sizeof(int)];
            }
        }

        rightIn->setIterator();

        //getData from iterator
        if (iterator->getNextTuple(leftBlock) != QE_EOF){
            getData(leftBlock, this->leftData, leftAttribute, leftPos, conType);
        }
    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        int leftRecordSize=leftAttribute.size();
        int rightRecordSize=rightAttribute.size();
        int outputRecordSize=attribute.size();
        int maxSize = pageNum*PAGE_SIZE;
        //use this check to check if we use all the data in the leftBlock yet
        while (true){
            if(readLeft){
                readLeft=false;
                int totalSize=0;
                while (totalSize<maxSize&&leftReturnVal!=QE_EOF){
                    //printRecord(leftAttribute, leftBlock);
                    leftReturnVal=iterator->getNextTuple(this->leftBlock);
                    if(leftReturnVal==QE_EOF){
                        break;
                    }
                    int leftOffset = ceil(leftRecordSize / 8.0);
                    //null indicator processing
                    int isnullLeft[leftRecordSize];
                    for (int i = 0; i < leftOffset; i++) {
                        char checkNull;
                        memcpy(&checkNull, (char *) this->leftBlock + i, 1);
                        bitset<8> nullBitset(checkNull);
                        for (int j = 0; j < 8 && 8 * i + j < leftRecordSize; j++) {
                            isnullLeft[8 * i + j] = nullBitset.test(7 - j);
                        }
                    }
                    if (isnullLeft[leftPos] == 0) {
                        getData(leftBlock, leftData, leftAttribute, leftPos, conType);
                    }
                    int currentSize;
                    if(conType == TypeVarChar){
                        int actualSize;
                        memcpy(&actualSize, (char*)this->leftData, sizeof(int));
                        currentSize = actualSize;
                    }
                    else{
                        currentSize = sizeof(int);
                    }
                    char* tempData = new char[currentSize];
                    string key;
                    if(conType == TypeVarChar) {
                        memcpy(tempData, (char*)leftData + sizeof(int), currentSize);
                        string val(tempData, currentSize);
                        key=val;
                    }
                    else{
                        memcpy(tempData, (char*)leftData, currentSize);
                        if(conType==TypeInt){
                            int val=*(int*)tempData;
                            key=to_string(val);
                        }
                        else{
                            float val=*(float *)tempData;
                            key=to_string(val);
                        }
                    }

                    //totalSize += currentSize;
                    int recordLength=getLength(leftAttribute, leftBlock);
                    void *recordData=new char[recordLength];
                    memcpy(recordData, leftBlock, recordLength);
                    totalSize+=recordLength;
                    if(bufferMap.find(key)==bufferMap.end()){
                        vector<void*>vec;
                        vec.push_back(recordData);
                        bufferMap.insert({key, vec});
                    }
                    else{
                        vector<void*>vec=bufferMap[key];
                        vec.push_back(recordData);
                        bufferMap.insert({key, vec});
                    }
                    //I don't know a better way to do this since if you need to read to check the size it will pass that value afterward
                }
                tableScan->setIterator();
            }

            //
            while (tableScan->getNextTuple(this->rightBlock) != QE_EOF) {
                getData(rightBlock, rightData, rightAttribute, rightPos, conType);
                int currentSize;
                if(conType == TypeVarChar){
                    int actualSize;
                    memcpy(&actualSize, (char*)this->rightData, sizeof(int));
                    currentSize = actualSize;
                }
                else{
                    currentSize = sizeof(int);
                }
                char* tempData = new char[currentSize];
                string key;
                if(conType == TypeVarChar) {
                    memcpy(tempData, (char*)rightData + sizeof(int), currentSize);
                    string val(tempData, currentSize);
                    key=val;
                }
                else{
                    memcpy(tempData, (char*)rightData, currentSize);
                    if(conType==TypeInt){
                        int val=*(int*)tempData;
                        key=to_string(val);
                    }
                    else{
                        float val=*(float *)tempData;
                        key=to_string(val);
                    }
                }
                bool result =false;
                if(bufferMap.find(key)!=bufferMap.end()){
                    vector<void*>vec=bufferMap[key];
                    if(mapVecPos<vec.size()){
                        leftBlock=vec[mapVecPos];
                        result=true;
                        mapVecPos++;
                    }
                    else{
                        mapVecPos=0;
                    }
                }
               // bool result = compareData(leftData, rightData, conType, op);
                if (result) {
                    string isnullString = "";

                    int leftOffset = ceil(leftRecordSize / 8.0);
                    //null indicator processing
                    int isnullLeft[leftRecordSize];
                    for (int i = 0; i < leftOffset; i++) {
                        char checkNull;
                        memcpy(&checkNull, (char *) this->leftBlock + i, 1);
                        bitset<8> nullBitset(checkNull);
                        for (int j = 0; j < 8 && 8 * i + j < leftRecordSize; j++) {
                            isnullLeft[8 * i + j] = nullBitset.test(7 - j);
                        }
                    }

                    int rightOffset = ceil(rightRecordSize / 8.0);
                    int isnullRight[rightRecordSize];
                    for (int i = 0; i < rightOffset; i++) {
                        char checkNull;
                        memcpy(&checkNull, (char *) this->rightBlock + i, 1);
                        bitset<8> nullBitset(checkNull);
                        for (int j = 0; j < 8 && 8 * i + j < rightRecordSize; j++) {
                            isnullRight[8 * i + j] = nullBitset.test(7 - j);
                        }
                    }
                    //copy left null indicator
                    for (int i = 0; i < leftRecordSize; i++) {
                        isnullString += to_string(isnullLeft[i]);
                    }
                    //copy right null indicator
                    for (int i = 0; i < rightRecordSize; i++) {
                        isnullString += to_string(isnullRight[i]);
                    }
                    unsigned int nullIndicationSize = ceil(outputRecordSize / 8.0);
                    int outputOffset = nullIndicationSize;
                    auto indicator = new unsigned char[nullIndicationSize];
                    for (int i = 0; i < nullIndicationSize; i++) {
                        unsigned int isnull = 0;
                        for (int j = 0; j < 8 && i * 8 + j < outputRecordSize; j++) {
                            if (isnullString.at(i * 8 + j) == '1') {
                                isnull += 1;
                            }
                            isnull = isnull << (unsigned) 1;
                        }
                        if ((i + 1) * 8 > outputRecordSize)
                            isnull = isnull << (unsigned) (8 * (i + 1) - outputRecordSize - 1);

                        indicator[i] = isnull;
                    }
                    memcpy((char *) data, indicator, nullIndicationSize);
                    unsigned int leftLength = getLength(leftAttribute, leftData);
                    unsigned int rightLength = getLength(rightAttribute, rightData);

                    memcpy((char *) data + outputOffset, (char *) leftBlock + leftOffset, leftLength);
                    outputOffset += leftLength;
                    memcpy((char *) data + outputOffset, (char *) rightBlock + rightOffset, rightLength);
                    //printRecord(attribute, data);
                    return 0;
                }
            }
            readLeft=true;
            if(leftReturnVal==QE_EOF){
                break;
            }
        }
        return QE_EOF;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attribute;
        return 0;
    }
    //BNLJoin END

    //INLJoin START
    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        iterator = leftIn;
        indexScan = rightIn;
        //get Attribute from leftIn and rightIn
        leftIn->getAttributes(leftAttribute);
        rightIn->getAttributes(rightAttribute);
        op = condition.op;
        conType = condition.rhsValue.type;
        this->leftBlock = new char[PAGE_SIZE];
        this->rightBlock = new char[PAGE_SIZE];
        //get the position of condition attribute for leftIn
        for(int i = 0; i < leftAttribute.size(); i++){
            //get all attribute from left table
            attribute.push_back(leftAttribute[i]);
            if (condition.lhsAttr == leftAttribute[i].name){
                leftPos = i;
                this->leftData = new char[leftAttribute[i].length + sizeof(int)];
            }
        }
        //get the position of condition attribute for rightIn
        for(int i = 0; i < rightAttribute.size(); i++){
            //get all attribute from right table
            attribute.push_back(rightAttribute[i]);
            if (condition.rhsAttr == rightAttribute[i].name) {
                rightPos = i;
                this->rightData = new char[rightAttribute[i].length + sizeof(int)];
            }
        }

        //rightIn->setIterator(nullptr, nullptr, true, true);
    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        int leftRecordSize=leftAttribute.size();
        int rightRecordSize=rightAttribute.size();
        int outputRecordSize=attribute.size();

        while(iterator->getNextTuple(this->leftBlock)!=QE_EOF) {
            //printRecord(leftAttribute, leftBlock);
            int leftOffset = ceil(leftRecordSize/8.0);
            //null indicator processing
            int isnullLeft[leftRecordSize];
            for (int i = 0; i < leftOffset; i++) {
                char checkNull;
                memcpy(&checkNull, (char *) this->leftBlock + i, 1);
                bitset<8> nullBitset(checkNull);
                for (int j = 0; j < 8 && 8 * i + j < leftRecordSize; j++) {
                    isnullLeft[8 * i + j] = nullBitset.test(7 - j);
                }
            }
            if(isnullLeft[leftPos]==0){
                getData(leftBlock, leftData, leftAttribute, leftPos, conType);
                indexScan->setIterator(leftData, leftData, true, true);
                while(indexScan->getNextTuple(this->rightBlock)!=QE_EOF){
                    //printRecord(rightAttribute, rightBlock);
                    string isnullString = "";
                    int rightOffset = ceil(rightRecordSize/8.0);
                    int isnullRight[rightRecordSize];
                    for (int i = 0; i < rightOffset; i++) {
                        char checkNull;
                        memcpy(&checkNull, (char *) this->rightBlock + i, 1);
                        bitset<8> nullBitset(checkNull);
                        for (int j = 0; j < 8 && 8 * i + j < rightRecordSize; j++) {
                            isnullRight[8 * i + j] = nullBitset.test(7 - j);
                        }
                    }
                    //copy left null indicator
                    for(int i=0;i<leftRecordSize;i++){
                        isnullString+=to_string(isnullLeft[i]);
                    }
                    //copy right null indicator
                    for(int i=0;i<rightRecordSize;i++){
                        isnullString+=to_string(isnullRight[i]);
                    }
                    unsigned int nullIndicationSize=ceil(outputRecordSize/8.0);
                    int outputOffset=nullIndicationSize;
                    auto indicator = new unsigned char[nullIndicationSize];
                    for (int i = 0; i < nullIndicationSize; i++) {
                        unsigned int isnull = 0;
                        for (int j = 0; j < 8 && i * 8 + j < outputRecordSize; j++) {
                            if (isnullString.at(i * 8 + j) == '1') {
                                isnull += 1;
                            }
                            isnull = isnull << (unsigned) 1;
                        }
                        if ((i + 1) * 8 > outputRecordSize)
                            isnull = isnull << (unsigned) (8 * (i + 1) - outputRecordSize - 1);

                        indicator[i] = isnull;
                    }
                    memcpy((char *) data, indicator, nullIndicationSize);
                    unsigned int leftLength = getLength(leftAttribute, leftData);
                    unsigned int rightLength = getLength(rightAttribute, rightData);
                    //need to do null stuff here? FIX

                    memcpy((char *)data+outputOffset, (char *)leftBlock+leftOffset, leftLength);
                    outputOffset+=leftLength;
                    memcpy((char*)data + outputOffset, (char *)rightBlock+rightOffset, rightLength);
                    //printRecord(attribute, data);
                    return 0;
                }
            }
        }

        return QE_EOF;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attribute;
        return 0;
    }
    //INLJoin END

    //Extra Credit
    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    //Aggregate START
    // "You can assume we do the aggregation on a numeric attribute (INT or REAL)."
    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        conType = aggAttr.type;
        this->op = op;
        aggregateAttribute = aggAttr;
        iterator = input;
        dataSize = 0;
        check = true;
        input->getAttributes(attribute);
        for (int i = 0; i < attribute.size(); i++) {
            Attribute temp = attribute[i];
            string name = temp.name;
            if(temp.type == TypeVarChar) dataSize += sizeof(int);
            if(name == aggAttr.name){
                position = i;
                dataSize += aggAttr.length;
                break;
            }
            dataSize += temp.length;
        }
    }
    //Extra Credit
    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {
    }

    RC Aggregate::getNextTuple(void *data) {
        //already run once testcase ask for it to fail if run again
        if(!check)
            return QE_EOF;
        check = false;
        if(op == MAX){
            return getMAX(data);
        }
        else if(op == MIN){
            return getMIN(data);
        }
        else if(op == COUNT){
            return getCOUNT(data);
        }
        else if(op == SUM){
            return getSUM(data);
        }
        else if(op == AVG){
            return getAVG(data);
        }
    }
    //this one ask for us to put in attribute like "MAX(left.B)" for max with attribute left.B
    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        Attribute temp;
        string returnString = "(" + this->attribute[position].name + ")";
        if(op == MAX){
            temp.name = "MAX" + returnString;
        }
        else if(op == MIN){
            temp.name = "MIN" + returnString;
        }
        else if(op == COUNT){
            temp.name = "COUNT" + returnString;
            temp.type = TypeInt;
        }
        else if(op == SUM){
            temp.name = "SUM" + returnString;
        }
        else if(op == AVG){
            temp.name = "AVG" + returnString;
            temp.type = TypeReal;
        }
        attrs.push_back(temp);
        return 0;
    }

    RC Aggregate::getMAX(void *data){
        char* attributeData = new char[dataSize];
        char* max = new char[sizeof(int)];
        if(conType == TypeInt){
            int temp = INT_MIN;
            memcpy(max, &temp, sizeof(int));
        }
        //TypeReal
        else{
            float temp = FLT_MIN;
            memcpy(max, &temp, sizeof(float));
        }
        while (iterator->getNextTuple(attributeData) != QE_EOF) {
            char* tempData = new char[sizeof(int)];
            getData(attributeData, tempData, attribute, position, conType);
            if(conType == TypeInt){
                int currentMax;
                int tempInt;
                memcpy(&currentMax, max, sizeof(int));
                memcpy(&tempInt, tempData, sizeof(int));
                //printf("currentMax: %d\n", currentMax);
                if(currentMax < tempInt){
                    memcpy(max, tempData, sizeof(int));
                }
            }
            else{
                float currentMax;
                float tempFloat;
                memcpy(&currentMax, max, sizeof(float));
                memcpy(&tempFloat, tempData, sizeof(float));
                if(currentMax < tempFloat){
                    memcpy(max, tempData, sizeof(float));
                }
            }
        }
        int zero = 0;
        memcpy(data, &zero, sizeof(char));
        memcpy((char*) data + 1, max, sizeof(int));
        return 0;
    }

    RC Aggregate::getMIN(void *data) {
        char* attributeData = new char[dataSize];
        char* min = new char[sizeof(int)];
        if(conType == TypeInt){
            int temp = INT_MAX;
            memcpy(min, &temp, sizeof(int));
        }
            //TypeReal
        else{
            float temp = FLT_MAX;
            memcpy(min, &temp, sizeof(float));
        }
        while (iterator->getNextTuple(attributeData) != QE_EOF) {
            char* tempData = new char[sizeof(int)];
            getData(attributeData, tempData, attribute, position, conType);
            if(conType == TypeInt){
                int currentMin;
                int tempInt;
                memcpy(&currentMin, min, sizeof(int));
                memcpy(&tempInt, tempData, sizeof(int));
                if(currentMin > tempInt){
                    memcpy(min, tempData, sizeof(int));
                }
            }
            else{
                float currentMin;
                float tempFloat;
                memcpy(&currentMin, min, sizeof(float));
                memcpy(&tempFloat, tempData, sizeof(float));
                if(currentMin > tempFloat){
                    memcpy(min, tempData, sizeof(float));
                }
            }
        }
        int zero = 0;
        memcpy(data, &zero, sizeof(char));
        memcpy((char*) data + 1, min, sizeof(int));
        return 0;
    }

    RC Aggregate::getCOUNT(void *data) {
        char* attributeData = new char[dataSize];
        int count = 0;
        while (iterator->getNextTuple(attributeData) != QE_EOF){
            count++;
        }
        int zero = 0;
        memcpy(data, &zero, sizeof(char));
        memcpy((char*) data + 1, &count, sizeof(int));
        return 0;
    }

    RC Aggregate::getSUM(void *data) {
        char* attributeData = new char[dataSize];
        float sumFloat = 0.0;
        int sumInt = 0;
        int type = -1;
        while (iterator->getNextTuple(attributeData) != QE_EOF) {
            char* tempData = new char[sizeof(int)];
            getData(attributeData, tempData, attribute, position, conType);
            if(conType == TypeInt){
                type = 0;
                int tempInt;
                memcpy(&tempInt, tempData, sizeof(int));
                sumInt += tempInt;
            }
            else{
                float tempFloat;
                memcpy(&tempFloat, tempData, sizeof(float));
                sumFloat += tempFloat;
            }
        }

        int zero = 0;
        memcpy(data, &zero, sizeof(char));
        if(type == 0) {
            memcpy((char*) data + 1, &sumInt, sizeof(int));
        }
        else
            memcpy((char*) data + 1, &sumFloat, sizeof(float));
        return 0;
    }

    RC Aggregate::getAVG(void *data) {
        char* attributeData = new char[dataSize];
        float sumFloat = 0.0;
        int sumInt = 0;
        int type = -1;
        int count = 0;
        while (iterator->getNextTuple(attributeData) != QE_EOF) {
            count++;
            char* tempData = new char[sizeof(int)];
            getData(attributeData, tempData, attribute, position, conType);
            if(conType == TypeInt){
                type = 0;
                int tempInt;
                memcpy(&tempInt, tempData, sizeof(int));
                sumInt += tempInt;
            }
            else{
                float tempFloat;
                memcpy(&tempFloat, tempData, sizeof(float));
                sumFloat += tempFloat;
            }
        }
        int zero = 0;
        memcpy(data, &zero, sizeof(char));
        if(type == 0) {
            float result = (float)sumInt/(float)count;
            printf("%f %f %f\n", (float) result, (float) sumInt, (float) count);
            memcpy((char*) data + 1, &result, sizeof(float));
        }
        else {
            float result = (float)sumFloat/(float)count;
            memcpy((char *) data + 1, &result, sizeof(float));
        }
        printf("result from AVG: %f\n", *(float*)((char*)data +1));
        return 0;
    }

    //Aggregate END

    // read data/attribute from a tuple
    void getData(void* inputData, void* outputData, vector<Attribute> attribute, int position, AttrType type){
        unsigned int record_size = attribute.size();
        int offset = ceil(record_size / 8.0);
        //get the correct offset
        for(int i = 0; i < position; i++){
            if(attribute[i].type == TypeVarChar){
                int length;
                memcpy(&length, (char *)inputData + offset, sizeof(int));
                offset += length;
            }
            offset += sizeof(int);
        }
        int attrsLength;
        if(type == TypeVarChar)
            attrsLength = *(int *)((char *)inputData + offset) + (int)sizeof(int);
        else
            attrsLength = sizeof(int);
        memset(outputData, 0, attrsLength);
        memcpy(outputData, (char *)inputData + offset, attrsLength);
    }

    /*
     * use to oompare attrLeft and attrRight for other functions
     * REF:
     * EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    */
    int compareData(void *attrLeft, void *attrRight, AttrType type, CompOp op){
        //int result = 0;
        bool result= false;
        if(type == TypeVarChar){
            //get the real string
            int leftLength = *(int *)attrLeft;
            string left((char *)attrLeft + sizeof(int), leftLength);
            int rightLength = *(int *)attrRight;
            string right((char *)attrRight + sizeof(int), rightLength);
            switch(op) {
                case EQ_OP: result = strcmp(left.c_str(), right.c_str()) == 0; break;
                case LT_OP: result = strcmp(left.c_str(), right.c_str()) < 0; break;
                case GT_OP: result = strcmp(left.c_str(), right.c_str()) > 0; break;
                case LE_OP: result = strcmp(left.c_str(), right.c_str()) <= 0; break;
                case GE_OP: result = strcmp(left.c_str(), right.c_str()) >= 0;break;
                case NE_OP: result = strcmp(left.c_str(), right.c_str()) != 0; break;
                case NO_OP: result = true; break;
            }
        }
        else if(type == TypeInt){
            int left = *(int*)attrLeft;
            int right = *(int*)attrRight;
            switch(op) {
                case EQ_OP: result = left == right; break;
                case LT_OP: result = left < right; break;
                case GT_OP: result = left > right; break;
                case LE_OP:
                    result = left <= right;
                    break;
                case GE_OP: result = left >= right; break;
                case NE_OP: result = left != right; break;
                case NO_OP: result = true; break;
            }
        }
        else{
            float left = *(float*)attrLeft;
            float right = *(float*)attrRight;
            switch(op) {
                case EQ_OP: result = left == right; break;
                case LT_OP: result = left < right; break;
                case GT_OP: result = left > right; break;
                case LE_OP: result = left <= right; break;
                case GE_OP: result = left >= right; break;
                case NE_OP: result = left != right; break;
                case NO_OP: result = true; break;
            }
        }
        //printf("result %d\n", result);
        return result;
    }

    //getLength from rbfm.cc
    /*
     * This function uses to get the real length of record that we are going to keep
     * return the size of the record
     */
    /*unsigned int getLength(const std::vector<Attribute> &recordDescriptor, const void *data) {
        unsigned int length = sizeof(int); // for storing the number of Attribute in recordDescriptor
        unsigned int current_pos = 0; //for storing the current position in data
        unsigned int record_size = recordDescriptor.size();
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
    }*/
    unsigned int getLength(const std::vector<Attribute> &attribute, const void *data){
        unsigned int record_size = attribute.size();
        int offset = ceil(record_size / 8.0);
        int length = 0;
        //get the correct offset
        for(int i = 0; i < record_size; i++){
            if(attribute[i].type == TypeVarChar){
                int actualLength;
                memcpy(&actualLength, (char *)data + offset, sizeof(int));
                offset += actualLength;
                length+=actualLength;
            }
            offset += sizeof(int);
            length+=sizeof(int);
        }
        return length;
    }

    unsigned int printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
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

} // namespace PeterDB
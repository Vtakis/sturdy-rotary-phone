#include <stdint.h>
typedef struct tuple tuple;
typedef struct relation relation;
typedef struct resultList resultList;
typedef struct resultNode resultNode;
typedef struct rowResult rowResult;
typedef struct histNode histNode;
typedef struct bucketNode bucketNode;
typedef struct chainNode chainNode;
typedef struct indexHT indexHT;
typedef struct hist hist;
#ifndef FUNCTIONS_H_
#define FUNCTIONS_H_

struct resultList
{
	resultNode *start;
	resultNode *end;
	int32_t numberOfNodes;
	int32_t numberOfResults;
};
struct resultNode
{
	rowResult *row_Array;
	resultNode *next;
	int32_t rowSize;
};
struct rowResult
{
	uint32_t idR;
	uint32_t idS;
};
struct tuple
{
	int32_t id;
	int32_t value;
};
struct relation
{
	tuple *tuples;
	uint32_t num_of_tuples;
};
struct hist
{
	histNode* histArray;
	int32_t histSize;
};
struct histNode
{
	tuple tuple;
	int count;
	int point;
};
struct bucketNode
{
	int lastChainPosition;
};
struct indexHT //bucket kai chain
{
	bucketNode* bucketArray;
	int bucketSize;
	chainNode* chainNode;
	int chainSize;
};
struct chainNode
{
	int bucketPos;
	int prevchainPosition;
};
unsigned int hash(int32_t x,int);
indexHT* initiliazeIndexHT(relation* ,int32_t);
void createRelations(int32_t[],uint32_t,int32_t[],uint32_t,relation **,relation**);
resultList* RadixHashJoin(relation *relR,relation *relS,int32_t,int32_t);
hist* createHistArray(relation **rel);
hist* createSumHistArray(hist *array);
relation* createReOrderedArray(relation *array,hist *sumArray);
void deleteHashTable(indexHT **);
indexHT* createHashTable(relation* reOrderedArray,int32_t start,int32_t end);
void compareRelations(indexHT *ht,relation *array,int32_t start,int32_t end,relation *hashedArray,resultList *resList,int32_t );
resultList *initializeResultList(void);
void insertResult(resultList *list,uint32_t id1,uint32_t id2,int32_t);
void printResults(resultList *list);
void createHT_CompareBuckets(resultList* ,hist*,hist*,relation*,relation*,int32_t,int32_t);
void writeFile(uint32_t,uint32_t);
void readFile(int32_t[],uint32_t *,int32_t[],uint32_t *);
void deleteResultList(resultList *);
#endif /* FUNCTIONS_H_ */







#include <stdint.h>
typedef struct tuple tuple;
typedef struct relation relation;
typedef struct result result;
typedef struct histNode histNode;
typedef struct bucketNode bucketNode;
typedef struct chainNode chainNode;
typedef struct indexHT indexHT;
typedef struct hist hist;
#ifndef FUNCTIONS_H_
#define FUNCTIONS_H_

struct result
{
	tuple *array_tuple;
	result *next;
	int32_t size;
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
result* RadixHashJoin(relation *relR,relation *relS);
hist* createHistArray(relation **rel);
hist* createSumHistArray(hist *array);
relation* createReOrderedArray(relation *array,hist *sumArray);
void deleteHashTable(indexHT **);
indexHT* createHashTable(relation* reOrderedArray,int32_t start,int32_t end);
void compareRelations(indexHT *bucketArray,relation* reOrdered_Bigger_Array,result* output);
#endif /* FUNCTIONS_H_ */

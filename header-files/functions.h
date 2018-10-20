#include <stdint.h>
typedef struct tuple tuple;
typedef struct relation relation;
typedef struct result result;
typedef struct histNode histNode;
typedef struct bucketNode bucketNode;
typedef struct chainNode chainNode;
typedef struct bucket bucket;
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
struct histNode
{
	tuple tuple;
	int count;
};
struct bucketNode
{
	int lastChainPosition;
	chainNode* chainNode;
};
struct bucket
{
	bucketNode* bucketArray;
	int bucketSize;
};
struct chainNode
{
	int bucketPos;
	int prevchainPosition;
};
result* RadixHashJoin(relation *relR,relation *relS);
histNode* createHistArray(relation *rel);
histNode* createSumHistArray(histNode *array);
relation* createReOrderedArray(histNode *sumArray);
int hashFunction();
bucket* createHashTable(relation* reOrderedArray);
void compareRelations(bucket *bucketArray,relation* reOrdered_Bigger_Array,result* output);
#endif /* FUNCTIONS_H_ */

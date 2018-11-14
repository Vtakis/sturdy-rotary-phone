#include <stdint.h>
typedef struct tuple tuple;
typedef struct multiColumnRelation multiColumnRelation;
typedef struct oneColumnRelation oneColumnRelation;
typedef struct resultList resultList;
typedef struct resultNode resultNode;
typedef struct rowResult rowResult;
typedef struct histNode histNode;
typedef struct bucketNode bucketNode;
typedef struct chainNode chainNode;
typedef struct indexHT indexHT;
typedef struct hist hist;
typedef struct queryDataIndex queryDataIndex;
typedef struct ListNode ListNode;
typedef struct RelColNode RelColNode;
typedef struct filterPredNode filterPredNode;
typedef struct joinPredNode joinPredNode;
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
struct multiColumnRelation//to struct anaferetai se enan kombo , ara kanw pinaka apo tetoia
{
	uint32_t rowCount;
	uint32_t colCount;
	uint64_t **table;//
};
struct oneColumnRelation//to palio relation , PREPEI NA ALLAKSW PANTOU TO ONOMA
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

////////////////////////////////////////////////////////
struct RelColNode //de borw na skeftw kalo onoma
{
	int rel;
	int col;
};

struct filterPredNode
{
	RelColNode *left;  //skalwsa-ti onoma na valw
	char typeOper;
	int value;
};

struct joinPredNode
{
	RelColNode *left;
	RelColNode *right;
};

struct queryDataIndex
{
	int numRelQuery;  //posa relations aforoun to query
	int *QueryRel;  //antistoixish relations twn predicators me ta pragmatika relations

	int numPredFilter;  //posa predicators filter
	filterPredNode *predFilter;  //pinakas me ta filter predicators

	int numPredJoin;  //posa predicators join
	joinPredNode *predJoin;  //pinakas me ta join predicators

	int numViewQuery;  //posa views zhtaei to query
	RelColNode *viewQuery;  //pinakas me ta views
};
///////////////////////////////////////////////////////////

unsigned int hash(int32_t x,int);
indexHT* initiliazeIndexHT(oneColumnRelation* ,int32_t);
void createRelations(int32_t[],uint32_t,int32_t[],uint32_t,oneColumnRelation **,oneColumnRelation**);
resultList* RadixHashJoin(oneColumnRelation *relR,oneColumnRelation *relS,int32_t,int32_t);
hist* createHistArray(oneColumnRelation **rel);
hist* createSumHistArray(hist *array);
oneColumnRelation* createReOrderedArray(oneColumnRelation *array,hist *sumArray);
void deleteHashTable(indexHT **);
indexHT* createHashTable(oneColumnRelation* reOrderedArray,int32_t start,int32_t end);
void compareRelations(indexHT *ht,oneColumnRelation *array,int32_t start,int32_t end,oneColumnRelation *hashedArray,resultList *resList,int32_t );
resultList *initializeResultList(void);
void insertResult(resultList *list,uint32_t id1,uint32_t id2,int32_t);
void printResults(resultList *list);
void createHT_CompareBuckets(resultList* ,hist*,hist*,oneColumnRelation*,oneColumnRelation*,int32_t,int32_t);
void writeFile(uint32_t,uint32_t);
void readFile(int32_t[],uint32_t *,int32_t[],uint32_t *);
void deleteResultList(resultList *);
char* readWorkFile(char *filename);
#endif /* FUNCTIONS_H_ */







#include <stdint.h>
#include <stdbool.h>
typedef struct tuple tuple;
typedef struct multiColumnRelation multiColumnRelation;
typedef struct oneColumnRelation oneColumnRelation;
typedef struct resultListForJoin resultListForJoin;
typedef struct resultNodeForJoin resultNodeForJoin;
typedef struct resultList resultList;
typedef struct resultNode resultNode;
typedef struct rowResult rowResult;
typedef struct histNode histNode;
typedef struct bucketNode bucketNode;
typedef struct chainNode chainNode;
typedef struct indexHT indexHT;
typedef struct hist hist;
typedef struct queryDataIndex queryDataIndex;
typedef struct RelColNode RelColNode;
typedef struct filterPredNode filterPredNode;
typedef struct joinPredNode joinPredNode;
typedef struct middleResults middleResults;
typedef struct statistics statistics;
typedef struct statistics_array statistics_array;
typedef struct stats stats;
typedef struct all_stats all_stats;
typedef struct bestTree bestTree;
typedef struct listnode listnode;
typedef struct relationData relationData;

//
#ifndef FUNCTIONS_H_
#define FUNCTIONS_H_

struct relationData{
	multiColumnRelation *relationArray;
	all_stats *statsArray;
	char **relation;
};
struct bestTree{//to offset+1 einai to posa rel exei to set
	listnode *startlist;
	listnode *endlist;
	listnode *BestNode;
};
struct listnode{
	int cost;
	int *set;//mege8os data->numRelQuery
	int *teams;//idio mege8os
	int teamCount;//poses diaforetikes omades exoume,den metraw tis omades me ena rel mesa
	int *seira;
	int seira_point;
	all_stats *local_stats;
	struct listnode *next;
};
struct stats
{
	uint64_t l;
	uint64_t u;
	uint64_t f;
	uint64_t d;
};
struct all_stats
{
	uint64_t rels;
	uint64_t *cols;
	stats **array_with_stats;
};
struct resultListForJoin
{
	resultNodeForJoin *start;
	resultNodeForJoin *end;
	int32_t numberOfNodes;
	int32_t numberOfResults;
};
struct statistics_array
{
	uint64_t relId;
	uint64_t numberOfCols;
	uint64_t *cols;
	statistics *colStatsArray;
};
struct resultNodeForJoin
{
	int32_t *row_Array;
	resultNode *next;
	int32_t rowSize;
};
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
struct statistics
{
	uint64_t min;
	uint64_t max;
	uint64_t average;
	uint64_t distinctValues;
	float possibilityOfDistinct;
};
struct multiColumnRelation	//to struct anaferetai se enan kombo , ara kanw pinaka apo tetoia
{
	uint32_t rowCount;
	uint32_t colCount;
	oneColumnRelation *columns;
};
struct oneColumnRelation
{
	tuple *tuples;
	uint32_t num_of_tuples;
	statistics stats;
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
struct middleResults
{
	int team;
	int relation;
	int relation_id;
	int *rowIds;
	int rowIdsNum;
	int fromArray;
	statistics stats;
};
////////////////////////////////////////////////////////
struct RelColNode //de borw na skeftw kalo onoma
{
	int rel;
	int col;
};

struct filterPredNode		//afora mono prakseis me arithmous
{
	RelColNode *relColumn;  //skalwsa-ti onoma na valw
	char typeOperation;
	int value;
};

struct joinPredNode
{
	RelColNode *left;
	RelColNode *right;
	int selected;
	int score;
};

struct queryDataIndex
{
	int numRelQuery;  //posa relations aforoun to query
	int *QueryRelArray;  //antistoixish relations twn predicators me ta pragmatika relations

	int numPredFilter;  //posa predicators filter
	filterPredNode *predFilterArray;  //pinakas me ta filter predicators

	int numPredJoinTwoRel;  //posa predicators join pou aforoun columns diaforetikou relation
	joinPredNode *twoRelationPredArray;  //pinakas me ta join predicators apo diaforetika relations

	int numViewQuery;  //posa views zhtaei to query
	RelColNode *viewQueryArray;  //pinakas me ta views
};
///////////////////////////////////////////////////////////

char **readRelations(int *);
void createRelations(int,multiColumnRelation **,all_stats **,char **);
void executeBatches(multiColumnRelation *,all_stats *);
int *JoinEnumeration(queryDataIndex *data,all_stats *statsArray);
void greaterThanFilterStatsCalculator(queryDataIndex *data,all_stats *statsArray,int *teams,int relationId,int columnIndx,int value);// R.A>k1
void lessThanFilterStatsCalculator(queryDataIndex *data,all_stats *statsArray,int *teams,int relationId,int columnIndx,int value);// R.A<k2
void sameJoinStatsCalculator(queryDataIndex *data,all_stats *statsArray,int *teams,int leftRelationId,int leftColumnIndx,int rightRelationId,int rightColumnIndx);
void RadixStatsCalculator(queryDataIndex *data,all_stats *statsArray,int *teams,int leftRelationId,int leftColumnIndx,int rightRelationId,int rightColumnIndx);
void print_stats_function(all_stats *statsArray);
void insertList(listnode **list,int cost,int *set,int size,int *teams,int teamCount,all_stats *stats,int *seira,int seira_size,int seira_point);
void printList(listnode *list,int max,int max_seira);
void deleteBestTree(bestTree *tree);
int createStatsAndFindPred(queryDataIndex *data,middleResults* middleResArray,int middleResultsCounter,multiColumnRelation* relationArray);
int checkIfOneRelationJoinExists(queryDataIndex *,middleResults *,int,int indx);
void setResultsToMiddleArray(resultList *list,middleResults *middleResultsArray,int index,int direction,int );
void printMiddleArray(middleResults *array,int size);
oneColumnRelation* setColumnFromMiddleArray(middleResults* middleResArray,int relationIndx,int columnIndx,int arrayIndx,multiColumnRelation* relationArray);
oneColumnRelation* setColumnFromFirstArray(multiColumnRelation* relationArray,int relationIndx,int columnIndx);
unsigned int hash(int32_t x,int);
indexHT* initiliazeIndexHT(oneColumnRelation* ,int32_t);
void createRelationsUT(int32_t[],uint32_t,int32_t[],uint32_t,oneColumnRelation **,oneColumnRelation**);
resultList* RadixHashJoin(oneColumnRelation *relR,oneColumnRelation *relS);
hist* createHistArray(oneColumnRelation **rel,int start,int end);
hist* createSumHistArray(hist *array);
void createReOrderedArray(oneColumnRelation *array,hist *sumArray,int start,int end,oneColumnRelation *);
void deleteHashTable(indexHT **);
indexHT* createHashTable(oneColumnRelation* reOrderedArray,int32_t start,int32_t end);
void compareRelations(indexHT *ht,oneColumnRelation *array,int32_t start,int32_t end,oneColumnRelation *hashedArray,resultList *resList,int32_t );
resultList *initializeResultList(void);
void insertResult(resultList *list,uint32_t id1,uint32_t id2,int32_t);
void printResults(resultList *list);
void createHT_CompareBuckets(resultList* ,hist*,hist*,oneColumnRelation*,oneColumnRelation*,int32_t,int32_t);//to last orisma einai boolean , apo poion pinaka erxete
void writeFile(uint32_t,uint32_t);
void readFile(int32_t[],int *,int32_t[],int *);
void deleteResultList(resultList *);
queryDataIndex* analyzeQuery(char * query);
void readWorkFile(char *filename,multiColumnRelation *,all_stats *);
queryDataIndex* addQueryData(char *token,int part);
middleResults executeFilter(oneColumnRelation*,int,char,int,all_stats *statsArray,int,int);
resultList* sameRelationJoin(oneColumnRelation *relR,oneColumnRelation *relS);
resultListForJoin *initializeResultListForJoin(void);
void insertResultForJoin(resultListForJoin *list,uint32_t id);
void printResultsForJoin(resultListForJoin *list);
void changeRowIdNumOfTeam(middleResults*,int,int,int,int);
int64_t SumOneColumnRelation(oneColumnRelation *R);
#endif /* FUNCTIONS_H_ */

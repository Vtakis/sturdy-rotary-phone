#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"
#define bucketPosNum 15
void createRelations(int32_t A[],uint32_t size_A,int32_t B[],uint32_t size_B,relation **S,relation **R){
	int32_t i;

	*S=malloc(sizeof(relation));
	(*S)->num_of_tuples=size_A;
	(*S)->tuples=malloc(size_A*sizeof(tuple));
	for(i=0;i<size_A;i++){
		(*S)->tuples[i].id=i+1;
		(*S)->tuples[i].value=A[i];
	}

	*R=malloc(sizeof(relation));
	(*R)->num_of_tuples=size_B;
	(*R)->tuples=malloc(size_B*sizeof(tuple));
	for(i=0;i<size_B;i++){
		(*R)->tuples[i].id=i+1;
		(*R)->tuples[i].value=B[i];
	}
	
}

histNode* createHistArray(relation **rel){
	int32_t i,j,*freq;
	int32_t count;
	histNode* HistNode;

	freq=malloc((*rel)->num_of_tuples*sizeof(int32_t));
	for(i=0;i<(*rel)->num_of_tuples;i++)
		freq[i]=-1;

	for(i=0;i<(*rel)->num_of_tuples;i++){
		count=1;
		for(j=i+1;j<(*rel)->num_of_tuples;j++){
			if((*rel)->tuples[i].value==(*rel)->tuples[j].value){
				count++;
				freq[j]=0;
			}
		}
		if(freq[i]!=0)
			freq[i]=count;
	}
	for(i=0;i<(*rel)->num_of_tuples;i++){
		if(freq[i]!=0)
			printf("%d: %d\n",(*rel)->tuples[i].value,freq[i]);
	}
	//Prepei na epistrefw pinaka typou histNode. Need to do it!

	return NULL;
}

unsigned int hash(int32_t x,int mod) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x%mod;
}
indexHT* createHashTable(relation* reOrderedArray){

	int32_t i;
	indexHT* indexht;
	indexht = initiliazeIndexHT(reOrderedArray);
	for(i=0;i<reOrderedArray->num_of_tuples;i++){
		if(indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition == -1)
		{
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = reOrderedArray->tuples[i].id;
		}
		else
		{
			indexht->chainNode[reOrderedArray->tuples[i].id].prevchainPosition = indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition;
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = indexht->chainNode[reOrderedArray->tuples[i].id].bucketPos;
		}
	}
	return indexht;
}
indexHT* initiliazeIndexHT(relation* reOrderedArray)
{
	int32_t i;
	indexHT* indexht = malloc(sizeof(indexHT));
	indexht->bucketArray = malloc(bucketPosNum*sizeof(bucketNode));
	indexht->chainNode = malloc(reOrderedArray->num_of_tuples*sizeof(chainNode));
	indexht->bucketSize = bucketPosNum;
	indexht->chainSize = reOrderedArray->num_of_tuples;
	for(i=0;i<bucketPosNum;i++){
		indexht->bucketArray[i].lastChainPosition = -1;
	}
	for(i=0;i<indexht->chainSize;i++){
		indexht->chainNode->prevchainPosition = -1;
		indexht->chainNode->bucketPos = i;
	}
	return indexht;
}

relation* createReOrderedArray(relation *array,int32_t *sumArray,int32_t sizeofsum){//mporei na mhn xreiazetai to sizeofsum
	relation *result;
	int32_t i;

	result=malloc(sizeof(relation));
	result->num_of_tuples=array->num_of_tuples;
	result->tuples=malloc(array->num_of_tuples*sizeof(tuple));
	
	for(i=0;i<array->num_of_tuples;i++){
		result->tuples[sumArray[array->tuples[i].value%sizeofsum]].id=array->tuples[i].id;
		result->tuples[sumArray[array->tuples[i].value%sizeofsum]++].value=array->tuples[i].value;
		//sumArray[array[i]%sizeofsum]++;
	}

	return result;
}

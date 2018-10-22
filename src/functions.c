#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "../header-files/functions.h"
#define bucketPosNum 15
#define N 3

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

hist* createHistArray(relation **rel){
	int32_t i,j,*freq;
	int32_t count;
	hist *Hist;
	Hist=malloc(sizeof(Hist));
	Hist->histSize=pow(2,N);
	Hist->histArray=malloc(Hist->histSize*sizeof(histNode));	
	for(i=0;i<N;i++){
		Hist->histArray[i].count=0;
		Hist->histArray[i].point=0;
	}

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
		if(freq[i]!=0){
			freq[i]=count;
			Hist->histArray[(*rel)->tuples[i].value%Hist->histSize].count+=freq[i];
		}
	}
	
	for(i=0;i<Hist->histSize;i++)
		printf("Hist[%d]=%d\n",i,Hist->histArray[i].count);

	return Hist;
}

hist* createSumHistArray(hist *array){
	int32_t i,nextBucket=0;
	hist *Hist;
	Hist=malloc(sizeof(Hist));
	Hist->histSize=pow(2,N);
	Hist->histArray=malloc(Hist->histSize*sizeof(histNode));
	
	for(i=0;i<N;i++)
		Hist->histArray[i].count=0;
		Hist->histArray[i].point=0;
	for(i=0;i<array->histSize;i++){
		if(i==0){
			nextBucket=array->histArray[i].count;
			Hist->histArray[i].count=0;
			Hist->histArray[i].point=0;
		}
		else{
			Hist->histArray[i].count=nextBucket;
			Hist->histArray[i].point=nextBucket;
			nextBucket+=array->histArray[i].count;
		}
	}
	printf("\nSum------------->\n");
	for(i=0;i<Hist->histSize;i++){
		printf("Hist[%d]=%d\n",i,Hist->histArray[i].count);
	}
	return Hist;
}

unsigned int hash(int32_t x,int mod) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x%mod;
}
indexHT* createHashTable(relation* reOrderedArray,int32_t start,int32_t end){

	int32_t i;
	indexHT* indexht;
	indexht = initiliazeIndexHT(reOrderedArray,end-start+1);
	printf("start=%d  end=%d\n",start,end);
	for(i=start;i<end;i++){
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
indexHT* initiliazeIndexHT(relation* reOrderedArray,int32_t chainNumSize)
{
	int32_t i;
	indexHT* indexht = malloc(sizeof(indexHT));
	indexht->bucketArray = malloc(bucketPosNum*sizeof(bucketNode));
	indexht->chainNode = malloc(chainNumSize*sizeof(chainNode));
	indexht->bucketSize = bucketPosNum;
	indexht->chainSize = chainNumSize;
	for(i=0;i<bucketPosNum;i++){
		indexht->bucketArray[i].lastChainPosition = -1;
	}
	for(i=0;i<indexht->chainSize;i++){
		indexht->chainNode->prevchainPosition = -1;
		indexht->chainNode->bucketPos = i;
	}
	return indexht;
}
void deleteHashTable(indexHT **ht)
{
	int32_t i;
	free((*ht)->bucketArray);				/*diagrafh tou pinaka bucket*/
	free((*ht)->chainNode);					/*diagrafh tou pinaka chain*/
	free((*ht));							/*diagrafh olhs ths domhs*/
}
relation* createReOrderedArray(relation *array,hist *sumArray){
	relation *result;
	int32_t i,start=0,end,counter=0;

	result=malloc(sizeof(relation));
	result->num_of_tuples=array->num_of_tuples;
	result->tuples=malloc(array->num_of_tuples*sizeof(tuple));

	for(i=0;i<array->num_of_tuples;i++){
		printf("*%d\n",sumArray->histArray[array->tuples[i].value%sumArray->histSize].point);
		//printf("--%d\n",sumArray->histArray[array->tuples[i].value%sumArray->histSize].count++);
		//printf("%d\n",sumArray->histArray[array->tuples[i].value%sumArray->histSize].count);
		//counter = sumArray->histArray[array->tuples[i].value%sumArray->histSize].count;
		result->tuples[sumArray->histArray[array->tuples[i].value%sumArray->histSize].point].id=array->tuples[i].id;
		result->tuples[sumArray->histArray[array->tuples[i].value%sumArray->histSize].point++].value=array->tuples[i].value;
		printf("**%d\n",sumArray->histArray[array->tuples[i].value%sumArray->histSize].point);
		//counter = sumArray->histArray[array->tuples[i].value%sumArray->histSize].count+1;*/

	}
	return result;
}


void compareRelations(indexHT *bucketArray,relation* reOrdered_Bigger_Array,result* output){

}

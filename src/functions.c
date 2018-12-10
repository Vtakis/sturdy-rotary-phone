#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "../header-files/functions.h"
#define bucketPosNum 50
#define N 12

void createRelations(int32_t A[],uint32_t size_A,int32_t B[],uint32_t size_B,oneColumnRelation **S,oneColumnRelation **R){
	int32_t i;

	*S=malloc(sizeof(oneColumnRelation));
	(*S)->num_of_tuples=size_A;
	(*S)->tuples=malloc(size_A*sizeof(tuple));
	for(i=0;i<size_A;i++){
		(*S)->tuples[i].id=i+1;
		(*S)->tuples[i].value=A[i];
	}
	*R=malloc(sizeof(oneColumnRelation));
	(*R)->num_of_tuples=size_B;
	(*R)->tuples=malloc(size_B*sizeof(tuple));
	for(i=0;i<size_B;i++){
		(*R)->tuples[i].id=i+1;
		(*R)->tuples[i].value=B[i];
	}	
}
hist* createHistArray(oneColumnRelation **rel){
	int32_t i;
	hist *Hist;
	Hist=malloc(sizeof(Hist));
	Hist->histSize=pow(2,N);
	Hist->histArray=malloc(Hist->histSize*sizeof(histNode));	
	for(i=0;i<Hist->histSize;i++){
		Hist->histArray[i].count=0;
		Hist->histArray[i].point=0;
	}
	for(i=0;i<(*rel)->num_of_tuples;i++){
		Hist->histArray[(*rel)->tuples[i].value%Hist->histSize].count++;
	}
	return Hist;
}
hist* createSumHistArray(hist *array){

	int32_t i,nextBucket=0;
	hist *Hist;
	Hist=malloc(sizeof(Hist));
	Hist->histSize=pow(2,N);
	Hist->histArray=malloc(Hist->histSize*sizeof(histNode));
	for(i=0;i<Hist->histSize;i++){
		Hist->histArray[i].count=0;
		Hist->histArray[i].point=0;
	}
	for(i=0;i<array->histSize;i++){
		if(i==0){//1o bucket - vriskoume ousiastika pou 8a arxizei to epomeno bucket
			nextBucket=array->histArray[i].count;
			Hist->histArray[i].count=0;
			Hist->histArray[i].point=0;
		}
		else{
			Hist->histArray[i].count=nextBucket;//pou arxizei to trexon bucket
			Hist->histArray[i].point=nextBucket;
			nextBucket+=array->histArray[i].count;//pou 8a arxizei to epomeno bucket
		}
	}
	free(array->histArray);
	free(array);
	return Hist;
}

oneColumnRelation* createReOrderedArray(oneColumnRelation *array,hist *sumArray){
	oneColumnRelation *reOrderedArray;
	int32_t i;
	reOrderedArray=malloc(sizeof(oneColumnRelation));
	reOrderedArray->num_of_tuples=array->num_of_tuples;
	reOrderedArray->tuples=malloc(array->num_of_tuples*sizeof(tuple));
	for(i=0;i<array->num_of_tuples;i++){//to point mou dixnei pou prepei na balw to stoixeio
		reOrderedArray->tuples[sumArray->histArray[array->tuples[i].value%sumArray->histSize].point].id=array->tuples[i].id;//koitazoume ton sumArray gia na brw to offset pou 8a balw to epomeno tuple
		reOrderedArray->tuples[sumArray->histArray[array->tuples[i].value%sumArray->histSize].point++].value=array->tuples[i].value;//thn deuterh fora pou bazoume to value kanw ++ gia na deixnei sthn epomenh kenh 8esh
	}
	return reOrderedArray;
}
unsigned int hash(int32_t x,int mod) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x%mod;
}
indexHT* initiliazeIndexHT(oneColumnRelation* reOrderedArray,int32_t chainNumSize)
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
		indexht->chainNode[i].prevchainPosition = -1;
		indexht->chainNode[i].bucketPos = -1;
	}
	return indexht;
}
indexHT* createHashTable(oneColumnRelation* reOrderedArray,int32_t start,int32_t end){

	int32_t i;
	indexHT* indexht;
	indexht = initiliazeIndexHT(reOrderedArray,end-start+1);
	for(i=start;i<=end;i++){
		if(indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition == -1)//elegxos an to h 8esh tou bucket einai kenh
		{
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = i-start;		//i-start gia na paroume thn swsth thesh apo to reorder
			indexht->chainNode[i-start].bucketPos=i;
		}
		else
		{
			indexht->chainNode[i-start].prevchainPosition = indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition;
			indexht->chainNode[i-start].bucketPos = i;
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = i-start;
		}
	}
	return indexht;
}
void deleteHashTable(indexHT **ht)
{
	free((*ht)->bucketArray);				/*diagrafh tou pinaka bucket*/
	free((*ht)->chainNode);					/*diagrafh tou pinaka chain*/
	free((*ht));							/*diagrafh olhs ths domhs*/
}
void compareRelations(indexHT *ht,oneColumnRelation *array,int32_t start,int32_t end,oneColumnRelation *hashedArray,resultList *resList,int32_t fromArray){
	int32_t i,chain_offset,bucket_offset;

	for(i=start;i<=end;i++){
		if(ht->bucketArray[hash(array->tuples[i].value,bucketPosNum)].lastChainPosition == -1){//an einai -1 tote den exoume match opote to agnwoume
			continue;
		}
		else{
			chain_offset=ht->bucketArray[hash(array->tuples[i].value,bucketPosNum)].lastChainPosition;//pairnw ws offset thn teleutaia 8esh tou chain
			bucket_offset=ht->chainNode[chain_offset].bucketPos;
			while(1){
				if(array->tuples[i].value==hashedArray->tuples[bucket_offset].value){//elegxoume an exoun idio value
					insertResult(resList,hashedArray->tuples[bucket_offset].id,array->tuples[i].id,fromArray);
				}
				chain_offset=ht->chainNode[chain_offset].prevchainPosition;//to neo offset einai apo to prevchainPosition
				if(chain_offset==-1)break;//den exei alla stoixeia na doume
				bucket_offset = ht->chainNode[chain_offset].bucketPos;
			}
		}
	}
}
resultList *initializeResultList(void){
	resultList *list=malloc(sizeof(resultList));
	list->start=NULL;
	list->end=NULL;
	list->numberOfNodes=0;
	list->numberOfResults=0;
	return list;
}
void insertResult(resultList *list,uint32_t id1,uint32_t id2,int32_t fromArray){
	int32_t numberoftuples=(1024*1024)/sizeof(rowResult);
	if(list->end==NULL){//kenh lista
		list->start=malloc(sizeof(resultNode));
		list->end=list->start;
		list->numberOfNodes=1;
		list->numberOfResults=1;

		list->start->row_Array=malloc(numberoftuples*sizeof(rowResult));
		list->start->next=NULL;
		list->start->rowSize=1;

		if(fromArray == 0){
			list->start->row_Array[0].idR=id1;
			list->start->row_Array[0].idS=id2;
		}else if(fromArray==1){
			list->start->row_Array[0].idR=id2;
			list->start->row_Array[0].idS=id1;
		}else{
			list->start->row_Array[0].idR=id1;
		}
	}
	else{
		list->numberOfResults++;
		if( numberoftuples > list->end->rowSize ){//exei xwro

			if(fromArray == 0){
				list->end->row_Array[list->end->rowSize].idR=id1;
				list->end->row_Array[list->end->rowSize].idS=id2;
			}else if(fromArray==1){
				list->end->row_Array[list->end->rowSize].idR=id2;
				list->end->row_Array[list->end->rowSize].idS=id1;
			}else{
				list->start->row_Array[0].idR=id1;
			}
			list->end->rowSize++;
		}
		else{//ftiaxnw neo kombo
			list->end->next=malloc(sizeof(resultNode));
			list->end=list->end->next;
			list->numberOfNodes++;

			list->end->row_Array=malloc(numberoftuples*sizeof(rowResult));
			list->end->next=NULL;
			list->end->rowSize=1;

			if(fromArray == 0){
				list->end->row_Array[0].idR=id1;
				list->end->row_Array[0].idS=id2;
			}else if(fromArray==1){
				list->end->row_Array[0].idR=id2;
				list->end->row_Array[0].idS=id1;
			}else{
				list->start->row_Array[0].idR=id1;
			}
		}
	}
}
void printResults(resultList *list){
	resultNode *temp;
	temp =list->start;
	printf("\nRowidR\t\tRowidS\n");
	while(temp!=NULL){
		for(int i=0;i<temp->rowSize;i++){
			printf("%d\t\t%d\n",temp->row_Array[i].idR,temp->row_Array[i].idS);
		}
		temp=temp->next;
	}
	printf("\n%d Results\n\n",list->numberOfResults);
}
void createHT_CompareBuckets(resultList* resList,hist *histSumArrayR,hist *histSumArrayS,oneColumnRelation *RR, oneColumnRelation *RS,int32_t size_A,int32_t size_B)
{
	indexHT* ht;
	int32_t startR=0,endR,startS=0,endS,counter = 0,cnt=0;
	while(1)
	{
		if(counter+1 > histSumArrayR->histSize)break;		//counter+1 giati einai h thesh gia to end ,dld an h thesh pou tha einai to end einai ektos oriwn break//
		startR = histSumArrayR->histArray[counter].count;
		startS = histSumArrayS->histArray[counter].count;
		if(counter+1==histSumArrayR->histSize){
			endR=size_A-1;
			endS=size_B-1;
		}else{
		endR = histSumArrayR->histArray[counter+1].count-1;
		endS = histSumArrayS->histArray[counter+1].count-1;
		}

		counter++;
		if(endR+1 == startR || endS+1 == startS) continue;  //elegxos an kapoio einai keno , opote den bgazei apotelesma
		cnt++;
		if((endR - startR) >= (endS - startS))//kanoume hash to pio mikro bucket
		{
			ht=createHashTable(RS,startS,endS);
			compareRelations(ht,RR,startR,endR,RS,resList,0);		// 0 -> hashedRs
			deleteHashTable(&ht);
		}
		else
		{
			ht=createHashTable(RR,startR,endR);
			compareRelations(ht,RS,startS,endS,RR,resList,1);		// 1 -> hashedRr
			deleteHashTable(&ht);
		}
	}
}
resultList* RadixHashJoin(oneColumnRelation *relR,oneColumnRelation *relS){

	hist *histSumArrayR,*histSumArrayS;
	oneColumnRelation *RS,*RR;
	resultList *resList;
	histSumArrayS=createSumHistArray(createHistArray(&relS));//dhmiourgia hist sum arrays
	histSumArrayR=createSumHistArray(createHistArray(&relR));
	RR=createReOrderedArray(relR,histSumArrayR);//dhmiourgia reordered array
	RS=createReOrderedArray(relS,histSumArrayS);
	resList=initializeResultList();
	indexHT* ht;
	int32_t startR=0,endR,startS=0,endS,counter = 0,cnt=0;
	while(1)
	{
		if(counter+1 > histSumArrayR->histSize)break;		//counter+1 giati einai h thesh gia to end ,dld an h thesh pou tha einai to end einai ektos oriwn break//
		startR = histSumArrayR->histArray[counter].count;
		startS = histSumArrayS->histArray[counter].count;
		if(counter+1==histSumArrayR->histSize){
			endR=relR->num_of_tuples-1;
			endS=relS->num_of_tuples-1;
		}else{
			endR = histSumArrayR->histArray[counter+1].count-1;
			endS = histSumArrayS->histArray[counter+1].count-1;
		}
		counter++;
		if(endR+1 == startR || endS+1 == startS) continue;  //elegxos an kapoio einai keno , opote den bgazei apotelesma
		cnt++;
		if((endR - startR) >= (endS - startS))//kanoume hash to pio mikro bucket
		{
			ht=createHashTable(RS,startS,endS);
			compareRelations(ht,RR,startR,endR,RS,resList,0);		// 0 -> hashedRs
			deleteHashTable(&ht);
		}
		else
		{
			ht=createHashTable(RR,startR,endR);
			compareRelations(ht,RS,startS,endS,RR,resList,1);		// 1 -> hashedRr
			deleteHashTable(&ht);
		}
	}
	free(histSumArrayR->histArray);
	free(histSumArrayR);
	free(histSumArrayS->histArray);
	free(histSumArrayS);

	free(RS->tuples);
	free(RS);
	free(RR->tuples);
	free(RR);

	free(relS->tuples);
	free(relS);
	free(relR->tuples);
	free(relR);

	return resList;
}
void writeFile(uint32_t size_A,uint32_t size_B){
	FILE *fp;
	fp=fopen("input-files/input.txt","w");
	fprintf(fp,"%d %d\n",size_A,size_B);
	for(int32_t i=0;i<size_A;i++)
	{
		fprintf(fp,"%d,",i);
	}
	fprintf(fp,"\n");
	for(int32_t i=0;i<size_B;i++)
	{
		fprintf(fp,"%d,",size_B-i);
	}
	fclose(fp);
}
void readFile(int32_t A[],uint32_t *size_A,int32_t B[],uint32_t *size_B){
	FILE *fp;
	fp=fopen("input-files/input.txt","r");
	int32_t el1,el2;
	fscanf(fp,"%d %d",size_A,size_B);

	for(int32_t i=0;i<*size_A;i++){
		fscanf(fp,"%d,",&el1);
		A[i]=el1;
	}
	for(int32_t i=0;i<*size_B;i++){
		fscanf(fp,"%d,",&el2);
		B[i]=el2;
	}
	fclose(fp);
}
void deleteResultList(resultList * reslist){
	resultNode *temp,*temp2;
	temp=reslist->start;
	while(temp!=NULL){
		free(temp->row_Array);
		temp2=temp;
		temp=temp->next;
		free(temp2);
	}
	free(reslist);
}
void readWorkFile(char *filename,multiColumnRelation *relationArray)
{
	FILE *query_f;
	query_f=fopen(filename,"r");
    char c='a';
	char *queryString;
	int teamCounter=1,counter=0,phrase_size=100,middleResultsSize=100,i,columnIndx,relationId,relationIndex,flag=0,middleResultsCounter=0,j,leftRelationId,rightRelationId,leftRelationIndx,leftTeam=0,rightTeam=0,rightRelationIndx,leftColumnIndx,rightColumnIndx;
	queryString = malloc(phrase_size*sizeof(char));
	middleResults *middleResArray=malloc(middleResultsSize*sizeof(middleResults));
	for(i=0;i<middleResultsSize;i++){
		middleResArray[i].team=0;
		middleResArray[i].relation=-1;
		middleResArray[i].rowIdsNum=0;
		middleResArray[i].fromArray=-1;
		middleResArray[i].relation_id=-1;
	}
	queryDataIndex *data;
	int y=1;
    while(c!=EOF)
    {
    	counter=0,phrase_size=100;
		c=fgetc(query_f);
		while(c!=EOF && c!='\n')
		{
			queryString[counter]=c;
			if(counter==phrase_size)
			{
				phrase_size*=2;
				queryString=realloc(queryString,phrase_size*sizeof(char));
			}
			counter++;
			c=fgetc(query_f);
		}
		queryString[counter]='\0';
		if(strcmp(queryString,"F") && c!=EOF)
		{
			oneColumnRelation *column;
			//printf("\n\n%d)%s\n",y,queryString);
			y++;
			data=analyzeQuery(queryString);
			if(data->numPredFilter>0)				//kanoume prwta tis prakseis me arithmous
			{
				for(i=0;i<data->numPredFilter;i++)		//apo 0 mexri ton arithmo twn filtrwn pou uparxoun//
				{
					relationId = data->predFilterArray[i].relColumn->rel;
					columnIndx= data->predFilterArray[i].relColumn->col;
					relationIndex = data->QueryRelArray[relationId];
					flag=0;
					if(middleResultsCounter>0)			//exoume endiamesa apotelesmata
					{
						for(j=0;j<middleResultsCounter;j++)			//trexoume ton pinaka me ta relations kai psaxnoume an uparxei antistoixia//
						{
							if(middleResArray[j].relation==relationIndex)
							{
								column=setColumnFromMiddleArray(middleResArray,relationIndex,columnIndx,j,relationArray);

								free(middleResArray[j].rowIds);
								int team =middleResArray[j].team;
								middleResArray[j]=executeFilter(column,data->predFilterArray[i].value,data->predFilterArray[i].typeOperation,relationIndex);
								middleResArray[j].team=team;
								middleResArray[j].relation_id=data->predFilterArray[i].relColumn->rel;

								///if(middleResArray[middleResultsCounter].relation==-1)
								{
									//free(middleResArray[middleResultsCounter].rowIds);
									//middleResultsCounter--;
								}

								flag=1;
								free(column->tuples);
								free(column);
								break;
							}
						}
						if(flag==1)continue;
					}

					//edw tha erthei an den uparxei kapoio apotelesma apo to middleResults h an uparxoun apotelesmata sto middleResult pou den mas ikanopoioun.ara ftiaxnoume neo RowID sto middleResult
					column = setColumnFromFirstArray(relationArray,relationIndex,columnIndx);

					middleResArray[middleResultsCounter++]=executeFilter(column,data->predFilterArray[i].value,data->predFilterArray[i].typeOperation,relationIndex);
					middleResArray[middleResultsCounter-1].relation_id=data->predFilterArray[i].relColumn->rel;
					middleResArray[middleResultsCounter-1].team=teamCounter++;

					middleResArray[middleResultsCounter-1].fromArray=0;
					if(middleResArray[middleResultsCounter-1].relation==-1)
					{
						//free(middleResArray[middleResultsCounter-1].rowIds);
						middleResultsCounter--;
					}
					free(column->tuples);
					free(column);
				}
			}
			if(data->numPredJoinTwoRel>0)			//join 2 columns//
			{
				leftTeam=0;
				rightTeam=0;
				int indx=0;
				int leftColumnPosInMiddleArray=-1,rightColumnPosInMiddleArray=-1;

				for(i=0;i<data->numPredJoinTwoRel;i++)
				{
					oneColumnRelation *leftColumn;
					oneColumnRelation *rightColumn;
				    clock_t start, end;
				    double cpu_time_used;
				    start = clock();
				    if(data->numPredJoinTwoRel!=1 )
				    {
				    	indx=checkIfOneRelationJoinExists(data,middleResArray,middleResultsCounter,i);
						if(indx==-1)			//eimaste se TwoRelationJoin kai theloume na vroume ta statistika gia kathe kathgorhma//
						{
							indx=createStatsAndFindPred(data,middleResArray,middleResultsCounter,relationArray);
						}
				    }
					end = clock();
					cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
					//printf("\nTime = %f\n",cpu_time_used);
					//printf("Indx=%d\n",indx);
					leftColumnPosInMiddleArray=-1;
					rightColumnPosInMiddleArray=-1;
					leftRelationId = data->twoRelationPredArray[indx].left->rel;					//pernoume ta stoixeia apo thn domh pou krataei ta predications data //
					leftColumnIndx =data->twoRelationPredArray[indx].left->col;
					rightRelationId = data->twoRelationPredArray[indx].right->rel;
					rightColumnIndx= data->twoRelationPredArray[indx].right->col;

					leftRelationIndx = data->QueryRelArray[leftRelationId];
					rightRelationIndx = data->QueryRelArray[rightRelationId];
					if(middleResultsCounter>0)			//exoume endiamesa apotelesmata
					{
						for(j=0;j<middleResultsCounter;j++)			//trexoume ton pinaka me ta relations kai psaxnoume an uparxei antistoixia//
						{
							if(middleResArray[j].relation==leftRelationIndx && middleResArray[j].relation_id==leftRelationId)
							{
								leftColumn=setColumnFromMiddleArray(middleResArray,leftRelationIndx,leftColumnIndx,j,relationArray);	//dhmiourgia aristerhs sthlhs apo to middle results
								leftColumnPosInMiddleArray=j;			//h thesh ths aristerhs sthlhs mesa ston middleArray
								middleResArray[j].fromArray=1;
								leftTeam=middleResArray[j].team;
							}
							if(middleResArray[j].relation==rightRelationIndx && middleResArray[j].relation_id==rightRelationId)
							{
								rightColumn=setColumnFromMiddleArray(middleResArray,rightRelationIndx,rightColumnIndx,j,relationArray);	//dhmiourgia deksias sthlhs apo to middle results
								rightColumnPosInMiddleArray=j;			//h thesh ths deksias sthlhs mesa ston middleArray
								middleResArray[j].fromArray=1;
								rightTeam=middleResArray[j].team;
							}
						}
					}
					if(leftColumnPosInMiddleArray==-1)
					{
						leftColumn = setColumnFromFirstArray(relationArray,leftRelationIndx,leftColumnIndx);		//dhmiourgia aristerhs sthlhs apo ton arxiko pinaka
						middleResArray[middleResultsCounter].fromArray=0;
					}

					if(rightColumnPosInMiddleArray==-1)		//an den uparxei mesa ston middle array pairnoume thn sthlh apo ton arxiko pinaka
					{
						rightColumn = setColumnFromFirstArray(relationArray,rightRelationIndx,rightColumnIndx);		//dhmiourgia deksias sthlhs apo ton arxiko pinaka
						middleResArray[middleResultsCounter].fromArray=0;
					}
					resultList *resultList1,*resultList2;
					if(leftRelationId==rightRelationId || (leftTeam==rightTeam && leftColumnPosInMiddleArray!=-1 && rightColumnPosInMiddleArray!=-1))			//JoinOneRelationArray
					{
						//printf("One Relation Join\n");
						resultList1=sameRelationJoin(leftColumn,rightColumn);
						data->twoRelationPredArray[indx].selected=1;
						if(leftColumnPosInMiddleArray==-1)			//an den uparxei to left column ston middle array
						{
							middleResArray[middleResultsCounter].relation=leftRelationIndx;
							middleResArray[middleResultsCounter].fromArray=0;
							setResultsToMiddleArray(resultList1,middleResArray,middleResultsCounter,0,middleResultsCounter);
							middleResultsCounter++;
							middleResArray[middleResultsCounter-1].team=teamCounter++;
						}

						if(leftColumnPosInMiddleArray!=-1)			//an uparxei to left column ston middle array
						{
							middleResArray[leftColumnPosInMiddleArray].relation=leftRelationIndx;
							setResultsToMiddleArray(resultList1,middleResArray,leftColumnPosInMiddleArray,0,middleResultsCounter);
							changeRowIdNumOfTeam(middleResArray,middleResArray[leftColumnPosInMiddleArray].team,resultList1->numberOfResults,middleResultsCounter);
						}
						deleteResultList(resultList1);
					}
					else// if(joinFlag==2)										//JointwoRelationArray
					{
					    clock_t start, end;
					    double cpu_time_used;
					    start = clock();
					 //   printf("%d.%d=%d.%d    Score=%d\n",data->twoRelationPredArray[indx].left->rel,data->twoRelationPredArray[indx].left->col,data->twoRelationPredArray[indx].right->rel,data->twoRelationPredArray[indx].right->col,data->twoRelationPredArray[indx].score);
						resultList2=RadixHashJoin(leftColumn,rightColumn);
						data->twoRelationPredArray[indx].selected=1;

						end = clock();
						cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
						//printf("Radix Time = %f\n",cpu_time_used);
						if(leftColumnPosInMiddleArray==-1 && rightColumnPosInMiddleArray==-1){	//ftiaxnoume neo team

							middleResArray[middleResultsCounter].relation=leftRelationIndx;
							middleResArray[middleResultsCounter].fromArray=0;
							middleResArray[middleResultsCounter].relation_id=leftRelationId;
							setResultsToMiddleArray(resultList2,middleResArray,middleResultsCounter,0,middleResultsCounter);
							middleResultsCounter++;

							middleResArray[middleResultsCounter].relation=rightRelationIndx;
							middleResArray[middleResultsCounter].fromArray=0;
							middleResArray[middleResultsCounter].relation_id=rightRelationId;
							setResultsToMiddleArray(resultList2,middleResArray,middleResultsCounter,1,middleResultsCounter);
							middleResultsCounter++;

							middleResArray[middleResultsCounter-1].team=teamCounter;
							middleResArray[middleResultsCounter-2].team=teamCounter++;
						}
						else if(leftColumnPosInMiddleArray==-1 && rightColumnPosInMiddleArray!=-1){ //an uparxei team gia right column
							middleResArray[middleResultsCounter].relation=leftRelationIndx;
							middleResArray[middleResultsCounter].fromArray=0;
							middleResArray[middleResultsCounter].relation_id=leftRelationId;
							setResultsToMiddleArray(resultList2,middleResArray,middleResultsCounter,0,middleResultsCounter);

							middleResultsCounter++;
							middleResArray[rightColumnPosInMiddleArray].relation=rightRelationIndx;
							middleResArray[rightColumnPosInMiddleArray].relation_id=rightRelationId;
							setResultsToMiddleArray(resultList2,middleResArray,rightColumnPosInMiddleArray,1,middleResultsCounter);
							middleResArray[middleResultsCounter-1].team = middleResArray[rightColumnPosInMiddleArray].team;
							changeRowIdNumOfTeam(middleResArray,middleResArray[rightColumnPosInMiddleArray].team,resultList2->numberOfResults,middleResultsCounter);
						}
						else if(leftColumnPosInMiddleArray!=-1 && rightColumnPosInMiddleArray==-1){ //an uparxei team gia left column

							middleResArray[leftColumnPosInMiddleArray].relation=leftRelationIndx;
							middleResArray[leftColumnPosInMiddleArray].relation_id=leftRelationId;
							setResultsToMiddleArray(resultList2,middleResArray,leftColumnPosInMiddleArray,0,middleResultsCounter);

							middleResArray[middleResultsCounter].relation=rightRelationIndx;
							middleResArray[middleResultsCounter].fromArray=0;
							middleResArray[middleResultsCounter].relation_id=rightRelationId;
							setResultsToMiddleArray(resultList2,middleResArray,middleResultsCounter,1,middleResultsCounter);
							middleResultsCounter++;

							middleResArray[middleResultsCounter-1].team = middleResArray[leftColumnPosInMiddleArray].team;
							changeRowIdNumOfTeam(middleResArray,middleResArray[leftColumnPosInMiddleArray].team,resultList2->numberOfResults,middleResultsCounter);

						}
						else{  //uparxei team kai gia ta 2 column --- psaxnoume gia mikrotero # of team
							middleResArray[leftColumnPosInMiddleArray].relation=leftRelationIndx;
							middleResArray[leftColumnPosInMiddleArray].relation_id=leftRelationId;
							setResultsToMiddleArray(resultList2,middleResArray,leftColumnPosInMiddleArray,0,middleResultsCounter);
							middleResArray[rightColumnPosInMiddleArray].relation=rightRelationIndx;
							middleResArray[rightColumnPosInMiddleArray].relation_id=rightRelationId;
							setResultsToMiddleArray(resultList2,middleResArray,rightColumnPosInMiddleArray,1,middleResultsCounter);

							if(middleResArray[leftColumnPosInMiddleArray].team <= middleResArray[rightColumnPosInMiddleArray].team){
								for(j=0;j<middleResultsCounter;j++){
									if(middleResArray[j].team==middleResArray[rightColumnPosInMiddleArray].team){
										middleResArray[j].team=middleResArray[leftColumnPosInMiddleArray].team;
									}
								}
								changeRowIdNumOfTeam(middleResArray,middleResArray[leftColumnPosInMiddleArray].team,resultList2->numberOfResults,middleResultsCounter);
							}
							else{
								for(j=0;j<middleResultsCounter;j++){
									if(middleResArray[j].team==middleResArray[leftColumnPosInMiddleArray].team){
										middleResArray[j].team=middleResArray[rightColumnPosInMiddleArray].team;
									}
								}
								changeRowIdNumOfTeam(middleResArray,middleResArray[rightColumnPosInMiddleArray].team,resultList2->numberOfResults,middleResultsCounter);
							}
						}
						deleteResultList(resultList2);
					}
					//free(leftColumn->tuples);
					//free(rightColumn->tuples);
					//free(leftColumn);
					//free(rightColumn);
					leftColumnPosInMiddleArray=-1;
					rightColumnPosInMiddleArray=-1;
				}
				teamCounter=1;
				if(i==data->numPredJoinTwoRel)
				{
					if(data->numViewQuery>0)
					{
						for(i=0;i<data->numViewQuery;i++)
						{
							columnIndx = data->viewQueryArray[i].col;
							relationId =data->viewQueryArray[i].rel;
							relationIndex = data->QueryRelArray[relationId];
							for(j=0;j<middleResultsCounter;j++)
							{
								if(middleResArray[j].relation==relationIndex && middleResArray[j].relation_id==relationId)
								{
									column=setColumnFromMiddleArray(middleResArray,relationIndex,columnIndx,j,relationArray);
									long sum=SumOneColumnRelation(column);
									if(sum==0)
										printf("NULL ");
									else
										printf("%ld ",sum);
									free(column->tuples);
									free(column);
								}
							}
						}
						printf("\n");
					}
				}
				for(j=0;j<middleResultsCounter;j++){
					free(middleResArray[j].rowIds);
					middleResArray[j].relation=-1;
					middleResArray[j].rowIdsNum=0;
					middleResArray[j].team=0;
					middleResArray[j].fromArray=-1;
					middleResArray[j].relation_id=-1;
				}
			}
			middleResultsCounter=0;
		}
		memset(queryString,0,strlen(queryString));
    }
    free(queryString);
    for(int i=0;i<data->numPredJoinTwoRel;i++)
    {
        free(data->twoRelationPredArray[i].left);
        free(data->twoRelationPredArray[i].right);
    }
    free(data->twoRelationPredArray);
    free(data->QueryRelArray);
    free(data->viewQueryArray);
    for(int i=0;i<data->numPredFilter;i++)
    {
        free(data->predFilterArray[i].relColumn);
    }
    free(data->predFilterArray);
    free(data);
    free(middleResArray);
}
int createStatsAndFindPred(queryDataIndex *data,middleResults* middleResArray,int middleResultsCounter,multiColumnRelation* relationArray)
{
	statistics_array *statsArray;
	statsArray=malloc(data->numRelQuery*sizeof(statistics_array));
	for(int i=0;i<data->numRelQuery;i++)
	{
		statsArray[i].numberOfCols=0;
	}

	int k,statsArrayCounter=data->numRelQuery,leftColumnIndx,rightRelationId,rightColumnIndx,leftRelationId,leftRelationIndx,rightRelationIndx,j,rightColumnPosInMiddleArray=-1,tempValue=-1,leftColumnPosInMiddleArray=-1;
	for(k=0;k<data->numPredJoinTwoRel;k++){
		if(data->twoRelationPredArray[k].selected==1)continue;

		leftColumnPosInMiddleArray=-1;
		rightColumnPosInMiddleArray=-1;
		leftRelationId = data->twoRelationPredArray[k].left->rel;					//pernoume ta stoixeia apo thn domh pou krataei ta predications data //
		leftColumnIndx =data->twoRelationPredArray[k].left->col;
		rightRelationId = data->twoRelationPredArray[k].right->rel;
		rightColumnIndx= data->twoRelationPredArray[k].right->col;

		leftRelationIndx = data->QueryRelArray[leftRelationId];
		rightRelationIndx = data->QueryRelArray[rightRelationId];

		int leftDistNum,rightDistNum;
		float leftDistPossibility,rightDistPossibility;
		int leftRowsNum,rightRowsNum;
		if(middleResultsCounter>0)			//exoume endiamesa apotelesmata
		{
			for(j=0;j<middleResultsCounter;j++)			//trexoume ton pinaka me ta relations kai psaxnoume an uparxei antistoixia//
			{
				if(middleResArray[j].relation==leftRelationIndx && middleResArray[j].relation_id==leftRelationId)
				{
					int result=createStatsFromMiddleArray(&statsArray,middleResArray,middleResultsCounter,relationArray,leftRelationIndx,leftColumnIndx,j,&statsArrayCounter,leftRelationId);
					leftColumnPosInMiddleArray=j;			//h thesh ths aristerhs sthlhs mesa ston middleArray
					middleResArray[j].fromArray=1;
					if(result==-1){
						return k;
					}
					leftDistNum=statsArray[leftRelationId].colStatsArray[leftColumnIndx].distinctValues;
					leftRowsNum=middleResArray[j].rowIdsNum;
					leftDistPossibility=(float)leftDistNum/(float)middleResArray[j].rowIdsNum;
				}
				if(middleResArray[j].relation==rightRelationIndx && middleResArray[j].relation_id==rightRelationId)
				{
					int result=createStatsFromMiddleArray(&statsArray,middleResArray,middleResultsCounter,relationArray,rightRelationIndx,rightColumnIndx,j,&statsArrayCounter,rightRelationId);
					if(result==-1){
						return k;
					}
					rightColumnPosInMiddleArray=j;			//h thesh ths deksias sthlhs mesa ston middleArray
					middleResArray[j].fromArray=1;
					rightDistNum=statsArray[rightRelationId].colStatsArray[rightColumnIndx].distinctValues;
					rightDistPossibility=(float)rightDistNum/(float)middleResArray[j].rowIdsNum;
					rightRowsNum = middleResArray[j].rowIdsNum;
				}
			}
		}
		if(leftColumnPosInMiddleArray==-1)
		{
			createStatsFromFirstArray(&statsArray,relationArray,leftRelationIndx,leftColumnIndx,&statsArrayCounter,leftRelationId);
			middleResArray[middleResultsCounter].fromArray=0;
			leftDistNum=statsArray[leftRelationId].colStatsArray[leftColumnIndx].distinctValues;
			leftDistPossibility=(float)leftDistNum/(float)relationArray[rightRelationIndx].rowCount;
			leftRowsNum= relationArray[rightRelationIndx].rowCount;
		}
		if(rightColumnPosInMiddleArray==-1)		//an den uparxei mesa ston middle array pairnoume thn sthlh apo ton arxiko pinaka
		{
			createStatsFromFirstArray(&statsArray,relationArray,rightRelationIndx,rightColumnIndx,&statsArrayCounter,rightRelationId);
			middleResArray[middleResultsCounter].fromArray=0;
			rightDistNum=statsArray[rightRelationId].colStatsArray[rightColumnIndx].distinctValues;
			rightDistPossibility=(float)rightDistNum/(float)relationArray[rightRelationIndx].rowCount;
			rightRowsNum = relationArray[rightRelationIndx].rowCount;
		}
		int leftMax=statsArray[leftRelationId].colStatsArray[leftColumnIndx].max,leftMin=statsArray[leftRelationId].colStatsArray[leftColumnIndx].min,leftAvrg=statsArray[leftRelationId].colStatsArray[leftColumnIndx].average;
		int rightMax=statsArray[rightRelationId].colStatsArray[rightColumnIndx].max,rightMin=statsArray[rightRelationId].colStatsArray[rightColumnIndx].min,rightAvrg=statsArray[rightRelationId].colStatsArray[rightColumnIndx].average;

		if(leftMin>rightMax || rightMin>leftMax)					//zero results//MIN MAX min max
		{
			return k;
		}
		else if((leftMin>=rightMin && leftMax<=rightMax))			// RMIN  min max   RMAX /// 100   200 300   1000  ///left=100/100 right=900/900  avrg=100-400
		{
			if(leftDistPossibility>rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=9;
			}
			if(leftDistPossibility<rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=8;
			}
			if(leftDistPossibility==rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=10;
			}
		}
		else if((rightMin>=leftMin && rightMax<=leftMax))			// min  Min Max  max ///
		{
			if(leftDistPossibility>rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=8;
			}
			if(leftDistPossibility<rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=9;
			}
			if(leftDistPossibility==rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=10;
			}
		}
		else if((leftMin>=rightMin && leftMax>=rightMax))			// /MIN min Max max// min Min max Max
		{
			if(leftDistPossibility>rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=12;
			}
			if(leftDistPossibility<rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=12;
			}
			if(leftDistPossibility==rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=11;
			}
		}
		else if( (rightMin>=leftMin && rightMax>=leftMax))			// /MIN min Max max// min Min max Max
		{
			if(leftDistPossibility>rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=12;
			}
			if(leftDistPossibility<rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=12;
			}
			if(leftDistPossibility==rightDistPossibility)
			{
				data->twoRelationPredArray[k].score=11;
			}
		}
	}
	int max=-1;
	int pos=0;
	for(k=0;k<data->numPredJoinTwoRel;k++){
		if(data->twoRelationPredArray[k].selected==1)continue;
		if(max<data->twoRelationPredArray[k].score || max==-1)
		{
			max=data->twoRelationPredArray[k].score;
			pos=k;
		}

	}
	for(k=0;k<statsArrayCounter;k++)
	{
		if(statsArray[k].numberOfCols!=0)
		{
			free(statsArray[k].cols);
			free(statsArray[k].colStatsArray);
		}
	}
	free(statsArray);
	return pos;
}
int createStatsFromMiddleArray(statistics_array **statsArray,middleResults *middleResArray,int middleResultsCounter,multiColumnRelation *relationArray,int relationIndx,int columnIndx,int arrayIndx,int *statsArrayCounter,int relationId)
{
	int statsArrayIndex=relationId;
	int index=middleResArray[arrayIndx].rowIds[0];
	if(middleResArray[arrayIndx].rowIdsNum==0)
	{
		return -1;
	}
	if((*statsArray)[statsArrayIndex].numberOfCols==0)
	{
		(*statsArray)[statsArrayIndex].cols=malloc(relationArray[relationIndx].colCount*sizeof(uint64_t));
		(*statsArray)[statsArrayIndex].numberOfCols=relationArray[relationIndx].colCount;
		(*statsArray)[statsArrayIndex].colStatsArray=malloc(relationArray[relationIndx].colCount*sizeof(statistics));
		for(int i=0;i<(*statsArray)[statsArrayIndex].numberOfCols;i++)
		{
			(*statsArray)[statsArrayIndex].cols[i]=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].max=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].min=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].average=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].possibilityOfDistinct=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].distinctValues=-1;
		}
	}
	(*statsArray)[statsArrayIndex].cols[columnIndx]=columnIndx;
	(*statsArray)[statsArrayIndex].relId=relationIndx;

	int max=relationArray[relationIndx].table[columnIndx][index],min=relationArray[relationIndx].table[columnIndx][index],average=0;

	///briskw tis diakrites time me hash
	/*int **dis_values,*pointers,*maxes;//einai hash
	int hash_num=10000,num_per_hash=10000,distinct_values=0;

	dis_values=malloc(hash_num*sizeof(int *));
	pointers=malloc(hash_num*sizeof(int));
	maxes=malloc(hash_num*sizeof(int));

	for(int k=0;k<hash_num;k++){
		maxes[k]=num_per_hash;
		dis_values[k]=malloc(num_per_hash*sizeof(int *));
		pointers[k]=0;
	}*/
	///
	for(int k=0;k<middleResArray[arrayIndx].rowIdsNum;k++)
	{
		 index=middleResArray[arrayIndx].rowIds[k];
		 if(max<relationArray[relationIndx].table[columnIndx][index])
		 {
			 max=relationArray[relationIndx].table[columnIndx][index];
		 }
		 if(min>relationArray[relationIndx].table[columnIndx][index])
		 {
			 min=relationArray[relationIndx].table[columnIndx][index];
		 }
		 average+=relationArray[relationIndx].table[columnIndx][index];

		//////////////////////
	/*	int curValue=relationArray[relationIndx].table[columnIndx][index]%hash_num;
		//pointers[curValue]++;
		int flag=1;
		for(int l=0;l<pointers[curValue];l++){
			if(dis_values[curValue][l]==relationArray[relationIndx].table[columnIndx][index]){
				flag=0;
				break;
			}
		}
		if(flag==1){
			if(pointers[curValue]<maxes[curValue]){
				dis_values[curValue][pointers[curValue]]=relationArray[relationIndx].table[columnIndx][index];
				pointers[curValue]++;
				distinct_values++;
			}
			else{//realloc
				printf("realloc disticnt middle\n");
				maxes[curValue]=maxes[curValue]*2;
				dis_values[curValue]=realloc(dis_values[curValue],maxes[curValue]);

				dis_values[curValue][pointers[curValue]]=relationArray[relationIndx].table[columnIndx][index];
				pointers[curValue]++;
				distinct_values++;
			}
		}*/
	}
	/*for(int k=0;k<hash_num;k++){
		free(dis_values[k]);
	}
	free(pointers);
	free(maxes);
	free(dis_values);*/
//test oti bgazw swsta distinct
	/*int ans=0,sum=0;
	for(int k=0;k<hash_num;k++){
		//printf("dis_values[%d] = (items=%d) ",k,pointers[k]);
		sum+=pointers[k];
		for(int j=0;j<pointers[k];j++){
			//printf("%d ,",dis_values[k][j]);
			for(int i=0;i<middleResArray[arrayIndx].rowIdsNum;i++){
				index=middleResArray[arrayIndx].rowIds[i];
				if(dis_values[k][j]==relationArray[relationIndx].table[columnIndx][index]){
					//printf("%d %d\n",dis_values[k][j],relationArray[relationIndx].table[columnIndx][i]);
					ans++;
				}
			}
		}
		//printf("\n");
	}*/

	average/=middleResArray[arrayIndx].rowIdsNum;
//toy hlia
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].average=average;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].max=max;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].min=min;
	int maxdiff=max-average,mindiff=average-min;
	int diff =maxdiff-mindiff;
	float maxPoss=(float)(max-min)/(float)maxdiff,minPoss=(float)(max-min)/(float)mindiff;
	float sumPoss;
	if(diff<0)
	{
		sumPoss=0.5+(float)((-1)*diff)/(float)(max-min);
	}
	else sumPoss=0.5+(float)(diff)/(float)(max-min);
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].possibilityOfDistinct=((float)(max-min)/(float)middleResArray[arrayIndx].rowIdsNum);
	float totalPos=sumPoss*(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].possibilityOfDistinct;
	int distinct_values2=middleResArray[arrayIndx].rowIdsNum*(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].possibilityOfDistinct;
	if(distinct_values2>middleResArray[arrayIndx].rowIdsNum)
	{
		distinct_values2=middleResArray[arrayIndx].rowIdsNum;
	}
	distinct_values2=middleResArray[arrayIndx].rowIdsNum*totalPos;
	if(distinct_values2>middleResArray[arrayIndx].rowIdsNum)
	{
		distinct_values2=middleResArray[arrayIndx].rowIdsNum;
	}
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].distinctValues=distinct_values2;
//toy hlia end
}

void createStatsFromFirstArray(statistics_array **statsArray,multiColumnRelation *relationArray,int relationIndx,int columnIndx,int *statsArrayCounter,int relationId)
{
	int statsArrayIndex=relationId,flag=0;
	if((*statsArray)[statsArrayIndex].numberOfCols==0)
	{
		(*statsArray)[statsArrayIndex].cols=malloc(relationArray[relationIndx].colCount*sizeof(uint64_t));
		(*statsArray)[statsArrayIndex].numberOfCols=relationArray[relationIndx].colCount;
		(*statsArray)[statsArrayIndex].colStatsArray=malloc(relationArray[relationIndx].colCount*sizeof(statistics));
		for(int i=0;i<(*statsArray)[statsArrayIndex].numberOfCols;i++)
		{
			(*statsArray)[statsArrayIndex].cols[i]=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].max=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].min=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].average=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].possibilityOfDistinct=-1;
			(*statsArray)[statsArrayIndex].colStatsArray[i].distinctValues=-1;
		}
	}

	(*statsArray)[statsArrayIndex].cols[columnIndx]=columnIndx;
	(*statsArray)[statsArrayIndex].relId=relationIndx;
	(*statsArray)[statsArrayIndex].cols[columnIndx]=columnIndx;

	int max=relationArray[relationIndx].table[columnIndx][0],min=relationArray[relationIndx].table[columnIndx][0],average=0;

	///briskw tis diakrites time me hash
	/*int **dis_values,*pointers,*maxes;//einai hash
	int hash_num=10000,num_per_hash=10000,distinct_values=0;

	dis_values=malloc(hash_num*sizeof(int *));
	pointers=malloc(hash_num*sizeof(int));
	maxes=malloc(hash_num*sizeof(int));

	for(int k=0;k<hash_num;k++){
		maxes[k]=num_per_hash;
		dis_values[k]=malloc(num_per_hash*sizeof(int *));
		pointers[k]=0;
	}*/
	//
	for(int j=0;j<relationArray[relationIndx].rowCount;j++)
	{
		if(max<relationArray[relationIndx].table[columnIndx][j])
		{
			max =relationArray[relationIndx].table[columnIndx][j];
		}
		if(min>relationArray[relationIndx].table[columnIndx][j])
		{
			min = relationArray[relationIndx].table[columnIndx][j];
		}
		average+=relationArray[relationIndx].table[columnIndx][j];

		//////////////////////
		/*int curValue=relationArray[relationIndx].table[columnIndx][j]%hash_num;
		
		int flag=1;
		for(int l=0;l<pointers[curValue];l++){
			if(dis_values[curValue][l]==relationArray[relationIndx].table[columnIndx][j]){
				flag=0;
				break;
			}
		}
		if(flag==1){
			if(pointers[curValue]<maxes[curValue]){
				dis_values[curValue][pointers[curValue]]=relationArray[relationIndx].table[columnIndx][j];
				pointers[curValue]++;
				distinct_values++;
			}
			else{//realloc
				printf("realloc disticnt first\n");
				maxes[curValue]=maxes[curValue]*2;
				dis_values[curValue]=realloc(dis_values[curValue],maxes[curValue]);

				dis_values[curValue][pointers[curValue]]=relationArray[relationIndx].table[columnIndx][j];
				pointers[curValue]++;
				distinct_values++;
			}
		}*/
		//pointers[curValue]++;
		////////////////////////

	}
	/*for(int k=0;k<hash_num;k++){
		free(dis_values[k]);
	}
	free(pointers);
	free(maxes);
	free(dis_values);*/
//test oti bgazw swsta distinct
/*	int ans=0,sum=0;
	for(int k=0;k<hash_num;k++){
		//printf("dis_values[%d] = (items=%d) ",k,pointers[k]);
		sum+=pointers[k];
		for(int j=0;j<pointers[k];j++){
			//printf("%d ,",dis_values[k][j]);
			for(int i=0;i<relationArray[relationIndx].rowCount;i++){
				if(dis_values[k][j]==relationArray[relationIndx].table[columnIndx][i]){
					//printf("%d %d\n",dis_values[k][j],relationArray[relationIndx].table[columnIndx][i]);
					ans++;
				}
			}
		}
		//printf("\n");
	}
*/
	average/=relationArray[relationIndx].rowCount;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].average=average;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].max=max;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].min=min;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].possibilityOfDistinct=((float)(max-min)/(float)relationArray[relationIndx].rowCount);

//toy hlia
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].average=average;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].max=max;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].min=min;
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].possibilityOfDistinct=((float)(max-min)/(float)relationArray[relationIndx].rowCount);

	int maxdiff=max-average,mindiff=average-min;
	int diff =maxdiff-mindiff;
	float maxPoss=(float)(max-min)/(float)maxdiff,minPoss=(float)(max-min)/(float)mindiff;
	float sumPoss;

	if(diff<0)
	{
		sumPoss=0.5+(float)((-1)*diff)/(float)(max-min);
	}
	else sumPoss=0.5+(float)(diff)/(float)(max-min);
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].possibilityOfDistinct=((float)(max-min)/(float)relationArray[relationIndx].rowCount);
	float totalPos=sumPoss*(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].possibilityOfDistinct;
	int distinct_values2=relationArray[relationIndx].rowCount*(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].possibilityOfDistinct;
	if(distinct_values2>relationArray[relationIndx].rowCount)
	{
		distinct_values2=relationArray[relationIndx].rowCount;
	}
	distinct_values2=relationArray[relationIndx].rowCount*totalPos;
	if(distinct_values2>relationArray[relationIndx].rowCount)
	{
		distinct_values2=relationArray[relationIndx].rowCount;
	}
	(*statsArray)[statsArrayIndex].colStatsArray[columnIndx].distinctValues=distinct_values2;
//tou hlia end
}
int checkIfOneRelationJoinExists(queryDataIndex* data,middleResults* middleResArray,int middleResultsCounter,int indx)
{
	int k,rightRelationId,leftRelationId,leftRelationIndx,rightRelationIndx,j,leftTeam=0,rightTeam=0,rightColumnPosInMiddleArray=-1,tempValue=-1,leftColumnPosInMiddleArray=-1;
	if(indx==data->numPredJoinTwoRel-1)
	{
		for(k=0;k<data->numPredJoinTwoRel;k++){
			if(data->twoRelationPredArray[k].selected==1)continue;
			else break;
		}
		return k;
	}
	for(k=0;k<data->numPredJoinTwoRel;k++){
		if(data->twoRelationPredArray[k].selected==1)continue;
		else if(tempValue==-1) tempValue=k;

		leftColumnPosInMiddleArray=-1;
		rightColumnPosInMiddleArray=-1;
		leftTeam=0;
		rightTeam=0;
		leftRelationId = data->twoRelationPredArray[k].left->rel;					//pernoume ta stoixeia apo thn domh pou krataei ta predications data //
		rightRelationId = data->twoRelationPredArray[k].right->rel;

		leftRelationIndx = data->QueryRelArray[leftRelationId];
		rightRelationIndx = data->QueryRelArray[rightRelationId];
		if(middleResultsCounter>0)			//exoume endiamesa apotelesmata
		{
			for(j=0;j<middleResultsCounter;j++)			//trexoume ton pinaka me ta relations kai psaxnoume an uparxei antistoixia//
			{
				if(middleResArray[j].relation==leftRelationIndx && middleResArray[j].relation_id==leftRelationId)
				{
					leftColumnPosInMiddleArray=j;			//h thesh ths aristerhs sthlhs mesa ston middleArray
					middleResArray[j].fromArray=1;
					leftTeam=middleResArray[j].team;
				}
				if(middleResArray[j].relation==rightRelationIndx && middleResArray[j].relation_id==rightRelationId)
				{
					rightColumnPosInMiddleArray=j;			//h thesh ths deksias sthlhs mesa ston middleArray
					middleResArray[j].fromArray=1;
					rightTeam=middleResArray[j].team;
				}
			}
		}
		if(leftRelationId==rightRelationId || (leftTeam==rightTeam && leftColumnPosInMiddleArray!=-1 && rightColumnPosInMiddleArray!=-1))
		{
			return k;
		}
	}
	return -1;
}
void changeRowIdNumOfTeam(middleResults *array,int team,int numRows,int countMiddleArray){
	int i;
	for(i=0;i<countMiddleArray;i++){
		if(array[i].team==team){
			array[i].rowIdsNum=numRows;
		}
	}
}
void printMiddleArray(middleResults *array,int size)
{
	for(int i=0;i<size;i++)
	{
		printf("Relation[%d]-->%d  RelationID %d  Rows %d  Teams %d\n",i,array[i].relation,array[i].relation_id,array[i].rowIdsNum,array[i].team);
		printf("STATS min=%ld max=%ld average=%ld\n",array[i].stats.min,array[i].stats.max,array[i].stats.average);
		for(int j=0;j<array[i].rowIdsNum;j++)
		{
			//printf("Row[%d]=%d\n",j,array[i].rowIds[j]);
		}
		//printf("\n");
	}
}
void setResultsToMiddleArray(resultList *list,middleResults *middleResultsArray,int index,int direction,int middleResultsCounter)
{
	resultNode *temp;
	int counter=0;
	temp =list->start;
	if(temp==NULL)
	{
		for(int j=0;j<middleResultsCounter;j++)
		{
			if(middleResultsArray[index].team==middleResultsArray[j].team)
			{
				middleResultsArray[j].rowIdsNum=0;
			}
		}
		middleResultsArray[index].rowIds=malloc(sizeof(int));
		return;
	}
	if(middleResultsArray[index].rowIdsNum==0){
		middleResultsArray[index].rowIds=malloc(list->numberOfResults*sizeof(int));
		middleResultsArray[index].rowIdsNum=list->numberOfResults;
	}
	if(middleResultsArray[index].rowIdsNum < list->numberOfResults){
		middleResultsArray[index].rowIds=realloc(middleResultsArray[index].rowIds,list->numberOfResults*sizeof(int));
	}
	middleResultsArray[index].rowIdsNum=list->numberOfResults;
	int teamSize=0;
	for(int j=0;j<middleResultsCounter;j++)
	{
		if(middleResultsArray[j].team==middleResultsArray[index].team)
		{
			teamSize++;
		}
	}
	int cnt=0,*tempArray;
	if(middleResultsArray[index].fromArray == 1)
		tempArray=malloc(list->numberOfResults*teamSize*sizeof(int));
	while(temp!=NULL){
		for(int i=0;i<temp->rowSize;i++){
			if(direction==0){
				if(middleResultsArray[index].fromArray==0)
					middleResultsArray[index].rowIds[counter]=temp->row_Array[i].idS;
				else if(middleResultsArray[index].fromArray == 1){
					for(int j=0;j<middleResultsCounter;j++)
					{
						if(middleResultsArray[j].team==middleResultsArray[index].team)
						{
							if(middleResultsArray[j].rowIdsNum < list->numberOfResults){

								middleResultsArray[j].rowIds=realloc(middleResultsArray[j].rowIds,list->numberOfResults*sizeof(int));
							}
							middleResultsArray[j].rowIdsNum=list->numberOfResults;
							tempArray[cnt]=middleResultsArray[j].rowIds[temp->row_Array[i].idS];
							cnt++;
						}
					}
				}

			}
			else{
				if(middleResultsArray[index].fromArray==0)
					middleResultsArray[index].rowIds[counter]=temp->row_Array[i].idR;
				else if(middleResultsArray[index].fromArray == 1){
					for(int j=0;j<middleResultsCounter;j++)
					{
						if(middleResultsArray[j].team==middleResultsArray[index].team)
						{
							if(middleResultsArray[j].rowIdsNum < list->numberOfResults ){
								middleResultsArray[j].rowIds=realloc(middleResultsArray[j].rowIds,list->numberOfResults*sizeof(int));
							}
							middleResultsArray[j].rowIdsNum=list->numberOfResults;
							tempArray[cnt]=middleResultsArray[j].rowIds[temp->row_Array[i].idR];
							cnt++;
						}
					}
				}
			}
			counter++;
		}
		temp=temp->next;
	}
	int rowIndx=0;
	counter=0;
	if(middleResultsArray[index].fromArray==1){
		for(int k=0;k<list->numberOfResults;k++){
			for(int j=0;j<middleResultsCounter;j++){
				if(middleResultsArray[index].team==middleResultsArray[j].team){
					//printf("j=%d relation=%d rowIndx=%d counter=%d\n",j,middleResultsArray[j].relation,rowIndx,tempArray[counter]);
					middleResultsArray[j].rowIds[rowIndx]=tempArray[counter];
					counter++;
				}
			}
			rowIndx++;
		}
		free(tempArray);
	}
}
oneColumnRelation* setColumnFromFirstArray(multiColumnRelation* relationArray,int relationIndx,int columnIndx){
	oneColumnRelation *temp;
	temp=malloc(sizeof(oneColumnRelation));
	temp->tuples=malloc(relationArray[relationIndx].rowCount*sizeof(tuple));
	temp->num_of_tuples=relationArray[relationIndx].rowCount;
	for(int j=0;j<relationArray[relationIndx].rowCount;j++)
	{
		temp->tuples[j].value=relationArray[relationIndx].table[columnIndx][j];		//h mporw na stelnw kateutheian to table[column]//vazw se mia sthlh thn sthlh apo to arxiko table me ta data//
		temp->tuples[j].id=j;
	}
	return temp;
}
oneColumnRelation* setColumnFromMiddleArray(middleResults* middleResArray,int relationIndx,int columnIndx,int arrayIndx,multiColumnRelation* relationArray){
	oneColumnRelation *temp;
	temp=malloc(sizeof(oneColumnRelation));
	if(middleResArray[arrayIndx].rowIdsNum==0){
		temp->tuples=malloc(sizeof(tuple));
	}else{
		temp->tuples=malloc(middleResArray[arrayIndx].rowIdsNum*sizeof(tuple));
	}
	temp->num_of_tuples=middleResArray[arrayIndx].rowIdsNum;
	for(int k=0;k<middleResArray[arrayIndx].rowIdsNum;k++)
	{
		int index=middleResArray[arrayIndx].rowIds[k];
		temp->tuples[k].value=relationArray[relationIndx].table[columnIndx][index];
		temp->tuples[k].id=k;
	}
	return temp;
}
queryDataIndex* analyzeQuery(char *query)
{
	char *token,*token2,*token3,*temp,*tempToken;
	char *tempView,*tok,*bufRel,*bufCol;
	int i,j,k,index,numRel=0,numView=0;
	int numPredFilter=0,numPredJoinTwoRel=0;

	token=strtok(query,"|");
	token2=strtok(NULL,"|");
	token3=strtok(NULL,"|");

	tempToken=malloc(128);
	tempView=malloc(128);

	queryDataIndex *queryData;
	queryData=malloc(sizeof(queryDataIndex));

	for(i=0;i<3;i++){
		memset(tempToken,0,128);
		if(i==0){//1st part of query
			token=strtok(query,"|");
			strcat(tempToken,token);
			temp=strtok(token," ");
			while(temp!=NULL){
				numRel++;
				temp=strtok(NULL," ");
			}
			queryData->numRelQuery=numRel;
			queryData->QueryRelArray=malloc(numRel*sizeof(int));
			temp=strtok(tempToken," ");
			for(j=0;j<numRel;j++){
				queryData->QueryRelArray[j]=atoi(temp);
				temp=strtok(NULL," ");
			}
		}
		else if(i==1){//2nd part of query
			strcat(tempToken,token2);
			temp=strtok(token2,"&");
			while(temp!=NULL){
				memset(tempView,0,128);
				strcat(tempView,temp);
				temp=strtok(NULL,"&");
				if((tok=strchr(tempView,'>'))!=NULL ||(tok=strchr(tempView,'<'))!=NULL || ( (tok=strchr(tempView,'='))!=NULL && (tok=strstr(tok,"."))==NULL) ){
					numPredFilter++;
				}
				else if((tok=strchr(tempView,'='))!=NULL){//peritto else if mallon
						numPredJoinTwoRel++;
				}
				else{
					printf("wrong\n");
				}
			}
			queryData->numPredFilter=numPredFilter;
			queryData->predFilterArray=malloc(queryData->numPredFilter*sizeof(filterPredNode));
			for(k=0;k<queryData->numPredFilter;k++)
				queryData->predFilterArray[k].relColumn=malloc(sizeof(RelColNode));

			queryData->numPredJoinTwoRel=numPredJoinTwoRel;
			queryData->numPredJoinOneRel=0;
			queryData->twoRelationPredArray=malloc(queryData->numPredJoinTwoRel*sizeof(joinPredNode));
			for(k=0;k<queryData->numPredJoinTwoRel;k++){
				queryData->twoRelationPredArray[k].left=malloc(sizeof(RelColNode));
				queryData->twoRelationPredArray[k].right=malloc(sizeof(RelColNode));
				queryData->twoRelationPredArray[k].selected=0;
				queryData->twoRelationPredArray[k].score=-1;
			}


			int countF=0,countTwoR=0;
			char oper,*bufVal,*tok2;
			temp=strtok(tempToken,"&");
			while(temp!=NULL){
				memset(tempView,0,128);
				strcat(tempView,temp);
				temp=strtok(NULL,"&");
				if((tok=strchr(tempView,'>'))!=NULL ||(tok=strchr(tempView,'<'))!=NULL || ( (tok=strchr(tempView,'='))!=NULL && (tok2=strstr(tok,"."))==NULL) ){
					index = (int)(tok - tempView);
					oper=tempView[index];
					bufVal=malloc(128);
					memset(bufVal,0,128);
					for(k=index+1;k<strlen(tempView);k++){
						bufVal[k-index-1]=tempView[k];
					}
					bufVal[strlen(tempView)]='\0';

					tok=strchr(tempView,'.');
					index = (int)(tok - tempView);

					bufRel=malloc(index);
					bufCol=malloc(index);

					for(k=0;k<index;k++){
						bufRel[k]=tempView[k];
						bufCol[k]=tempView[k+index+1];
					}
					bufRel[index]='\0';
					bufCol[index]='\0';

					queryData->predFilterArray[countF].relColumn->rel=atoi(bufRel);
					queryData->predFilterArray[countF].relColumn->col=atoi(bufCol);
					queryData->predFilterArray[countF].typeOperation=oper;
					queryData->predFilterArray[countF].value=atoi(bufVal);

					countF++;
					free(bufVal);
					free(bufCol);
					free(bufRel);
				}
				else{//gia '='
					tok=strchr(tempView,'=');
					index = (int)(tok - tempView);
					int index_e=index;

					bufVal=malloc(index);
					for(k=0;k<index;k++){
						bufVal[k]=tempView[k];
					}
					bufVal[index]='\0';
					tok=strchr(bufVal,'.');
					index = (int)(tok - bufVal);

					bufRel=malloc(index);
					bufCol=malloc(index);
					for(k=0;k<index;k++){
						bufRel[k]=bufVal[k];
						bufCol[k]=bufVal[k+index+1];
					}
					bufRel[index]='\0';
					bufCol[index]='\0';

					queryData->twoRelationPredArray[countTwoR].left->rel=atoi(bufRel);
					queryData->twoRelationPredArray[countTwoR].left->col=atoi(bufCol);

					free(bufRel);
					free(bufCol);
					free(bufVal);
					////////////////////////////////////////////////////////////////
					bufVal=malloc(index_e);
					for(k=index_e+1;k<strlen(tempView);k++){
						bufVal[k-index_e-1]=tempView[k];
					}
					bufVal[index_e]='\0';
					tok=strchr(bufVal,'.');
					index = (int)(tok - bufVal);
					bufRel=malloc(index);
					bufCol=malloc(index);
					for(k=0;k<index;k++){
						bufRel[k]=bufVal[k];
						bufCol[k]=bufVal[k+index+1];
					}
					bufRel[index]='\0';
					bufCol[index]='\0';
					queryData->twoRelationPredArray[countTwoR].right->rel=atoi(bufRel);
					queryData->twoRelationPredArray[countTwoR].right->col=atoi(bufCol);
					//////////////////////////////////////////////////////////////
					countTwoR++;
					free(bufRel);
					free(bufCol);
					free(bufVal);

				}
			}
		}
		else{//3rd part of query
			strcat(tempToken,token3);
			temp=strtok(token3," ");
			while(temp!=NULL){
				numView++;
				temp=strtok(NULL," ");
			}
			queryData->numViewQuery=numView;
			queryData->viewQueryArray=malloc(numView*sizeof(RelColNode));
			temp=strtok(tempToken," ");
			for(j=0;j<numView;j++){
				memset(tempView,0,128);
				strcat(tempView,temp);
				temp=strtok(NULL," ");

				tok = strchr(tempView, '.');
				index = (int)(tok - tempView);

				bufRel=malloc(index);
				bufCol=malloc(index);

				for(k=0;k<index;k++){
					bufRel[k]=tempView[k];
					bufCol[k]=tempView[k+index+1];
				}
				bufRel[index]='\0';
				bufCol[index]='\0';
				queryData->viewQueryArray[j].rel=atoi(bufRel);
				queryData->viewQueryArray[j].col=atoi(bufCol);
				free(bufCol);
				free(bufRel);
			}
		}
	}
	free(tempToken);
	free(tempView);
	return queryData;
}
resultList* sameRelationJoin(oneColumnRelation *relR,oneColumnRelation *relS){//exoun to idio size
	int i;
	resultList *resList;
	resList=initializeResultList();

	for(i=0;i<relR->num_of_tuples;i++){
		if(relR->tuples[i].value==relS->tuples[i].value){//prepei ta id na einai ta idia
			insertResult(resList,relR->tuples[i].id,relR->tuples[i].id,1);			//2 sto teleutaio orisma shmainei oti exoume join apo idio relation
		}
	}
	return resList;
}
middleResults executeFilter(oneColumnRelation* column,int value,char typeOperation,int relation)
{
	int i;
	middleResults results;
	results.rowIds=malloc(column->num_of_tuples*sizeof(int));				//isws einai megalos o xwros apothikeushs//
	results.rowIdsNum=0;
	results.relation=relation;
	results.fromArray=0;

	for(i=0;i<column->num_of_tuples;i++)
	{
		if(typeOperation=='=')
		{	if(column->tuples[i].value==value){
				results.rowIds[results.rowIdsNum]=column->tuples[i].id;
				results.relation = relation;
				results.rowIdsNum++;
			}
		}
		else if(typeOperation=='>')
		{
			if(column->tuples[i].value>value){
				results.rowIds[results.rowIdsNum]=column->tuples[i].id;
				results.relation = relation;
				results.rowIdsNum++;
			}
		}
		else if(typeOperation=='<')
		{
			if(column->tuples[i].value<value){
				results.rowIds[results.rowIdsNum]=column->tuples[i].id;
				results.relation = relation;
				results.rowIdsNum++;
			}
		}
	}
	return results;
}
int64_t SumOneColumnRelation(oneColumnRelation *R){
	int64_t sum=0,i;

	for(i=0;i<R->num_of_tuples;i++){
		sum+=R->tuples[i].value;
	}

	return sum;
}

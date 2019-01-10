#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void executeBatches(multiColumnRelation *relationArray,all_stats *statsArray){
	char buffer[4096];
	char *token,*queryString;
	while(1)
	{
		memset(buffer,0,sizeof(buffer));
		ssize_t bytes = read(STDIN_FILENO,buffer,sizeof(buffer));
		if(bytes==0)		//to programma den diavase allo batch ara termatizei
		{
			break;
		}
		int numBatchQueries=1,cur=0;
		token=strtok(buffer,"\n");

		char **execQueries;
		execQueries=malloc(sizeof(char*));
		execQueries[0]=malloc(strlen(token)+1);
		strcpy(execQueries[0],token);

		while(token!=NULL){
			token=strtok(NULL,"\n");
			if(token==NULL || strcmp(token,"F")==0)
				break;
			execQueries=realloc(execQueries,(numBatchQueries+1)*sizeof(char*));
			execQueries[numBatchQueries]=malloc(strlen(token)+1);
			strcpy(execQueries[numBatchQueries++],token);
		}
		char c='a';
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
		//while(queryString!=NULL){
		for(cur=0;cur<numBatchQueries;cur++){
				oneColumnRelation *column;
				//printf("query:%s\n",queryString);
				//printf("\n\n%d)%s\n",y,queryString);
				y++;
				data=analyzeQuery(execQueries[cur]);
				//ftiaxnw enan mikrotero pinaka me ta stastika pou xreiazomai , mporei na exw kapoio relation 2 fores
				all_stats *temp_stats;
				temp_stats=malloc(sizeof(all_stats));
				temp_stats->rels=data->numRelQuery;
				temp_stats->cols=malloc(temp_stats->rels*sizeof(uint64_t));
				temp_stats->array_with_stats=malloc(temp_stats->rels*sizeof(stats *));
				for(i=0;i<temp_stats->rels;i++){
					//temp_stats->cols[i]=relationArray[data->QueryRelArray[i]].colCount;
					temp_stats->cols[i]=statsArray->cols[data->QueryRelArray[i]];
					//printf("QQQQQQQ %ld %ld\n",relationArray[data->QueryRelArray[i]].colCount,statsArray->cols[data->QueryRelArray[i]]);
					if(relationArray[data->QueryRelArray[i]].colCount!=statsArray->cols[data->QueryRelArray[i]]){
						//printf("PR0BLEM");
						//sleep(20);
					}

					temp_stats->array_with_stats[i]=malloc(temp_stats->cols[i]*sizeof(stats));
					for(j=0;j<temp_stats->cols[i];j++){
						temp_stats->array_with_stats[i][j].l=statsArray->array_with_stats[data->QueryRelArray[i]][j].l;
						temp_stats->array_with_stats[i][j].u=statsArray->array_with_stats[data->QueryRelArray[i]][j].u;
						temp_stats->array_with_stats[i][j].f=statsArray->array_with_stats[data->QueryRelArray[i]][j].f;
						temp_stats->array_with_stats[i][j].d=statsArray->array_with_stats[data->QueryRelArray[i]][j].d;
					}
				}

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
									middleResArray[j]=executeFilter(column,data->predFilterArray[i].value,data->predFilterArray[i].typeOperation,relationIndex,temp_stats,relationId,columnIndx);
									middleResArray[j].team=team;
									middleResArray[j].relation_id=data->predFilterArray[i].relColumn->rel;

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
						middleResArray[middleResultsCounter++]=executeFilter(column,data->predFilterArray[i].value,data->predFilterArray[i].typeOperation,relationIndex,temp_stats,relationId,columnIndx);
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
					//kalw thn sun gia na dw thn seira
					int *seira;
					seira=malloc(data->numPredJoinTwoRel*sizeof(int));
					//seira=JoinEnumeration(data,temp_stats);
					leftTeam=0;
					rightTeam=0;
					int indx=0;
					int leftColumnPosInMiddleArray=-1,rightColumnPosInMiddleArray=-1;

					for(i=0;i<data->numPredJoinTwoRel;i++){//
						indx=i;
						//indx=seira[i];//
					//for(i=0;i<data->numPredJoinTwoRel;i++)
					//{
						oneColumnRelation *leftColumn;
						oneColumnRelation *rightColumn;
						clock_t start, end;
						double cpu_time_used;
						start = clock();
						////edw dialegw thn seira twn kathgorhmatwn
					   /* if(data->numPredJoinTwoRel!=1 )
						{
							indx=checkIfOneRelationJoinExists(data,middleResArray,middleResultsCounter,i);
							if(indx==-1)			//eimaste se TwoRelationJoin kai theloume na vroume ta statistika gia kathe kathgorhma//
							{
								indx=createStatsAndFindPred(data,middleResArray,middleResultsCounter,relationArray);
							}
						}*/
	////end - edw dialegw thn seira twn kathgorhmatwn
						//FILE * fp;
						//fp=fopen("test.txt","w");
						//fprintf(stdout,"index=%d\n",indx);
						//fclose(fp);
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
								changeRowIdNumOfTeam(middleResArray,middleResArray[leftColumnPosInMiddleArray].team,resultList1->numberOfResults,middleResultsCounter,middleResArray[leftColumnPosInMiddleArray].team);
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
								int team;
								team=middleResArray[middleResultsCounter-1].team;
								middleResArray[middleResultsCounter-1].team = middleResArray[rightColumnPosInMiddleArray].team;
								changeRowIdNumOfTeam(middleResArray,team,resultList2->numberOfResults,middleResultsCounter,middleResArray[rightColumnPosInMiddleArray].team);
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
								int team=middleResArray[middleResultsCounter-1].team;
								middleResArray[middleResultsCounter-1].team = middleResArray[leftColumnPosInMiddleArray].team;
								changeRowIdNumOfTeam(middleResArray,team,resultList2->numberOfResults,middleResultsCounter,middleResArray[leftColumnPosInMiddleArray].team);

							}
							else{  //uparxei team kai gia ta 2 column --- psaxnoume gia mikrotero # of team
								middleResArray[leftColumnPosInMiddleArray].relation=leftRelationIndx;
								middleResArray[leftColumnPosInMiddleArray].relation_id=leftRelationId;
								setResultsToMiddleArray(resultList2,middleResArray,leftColumnPosInMiddleArray,0,middleResultsCounter);
								middleResArray[rightColumnPosInMiddleArray].relation=rightRelationIndx;
								middleResArray[rightColumnPosInMiddleArray].relation_id=rightRelationId;
								setResultsToMiddleArray(resultList2,middleResArray,rightColumnPosInMiddleArray,1,middleResultsCounter);
								int team;
								if(middleResArray[leftColumnPosInMiddleArray].team <= middleResArray[rightColumnPosInMiddleArray].team){
									for(j=0;j<middleResultsCounter;j++){
										if(middleResArray[j].team==middleResArray[rightColumnPosInMiddleArray].team){
											team=middleResArray[j].team;
											middleResArray[j].team=middleResArray[leftColumnPosInMiddleArray].team;
										}
									}
									changeRowIdNumOfTeam(middleResArray,team,resultList2->numberOfResults,middleResultsCounter,middleResArray[leftColumnPosInMiddleArray].team);
								}
								else{
									for(j=0;j<middleResultsCounter;j++){
										if(middleResArray[j].team==middleResArray[leftColumnPosInMiddleArray].team){
											team=middleResArray[j].team;
											middleResArray[j].team=middleResArray[rightColumnPosInMiddleArray].team;
										}
									}
									changeRowIdNumOfTeam(middleResArray,team,resultList2->numberOfResults,middleResultsCounter,middleResArray[rightColumnPosInMiddleArray].team);
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
							char *answer=malloc(100*sizeof(char));
							//answer=NULL;
							memset(answer,0,strlen(answer));
							for(i=0;i<data->numViewQuery;i++)
							{
								columnIndx = data->viewQueryArray[i].col;
								relationId =data->viewQueryArray[i].rel;
								relationIndex = data->QueryRelArray[relationId];
								for(j=0;j<middleResultsCounter;j++)
								{
									if(middleResArray[j].relation==relationIndex && middleResArray[j].relation_id==relationId)
									{
										clock_t start, end;
										double cpu_time_used;
										start = clock();
										int arrayIndx=j;
										long sum=0;
										for(int k=0;k<middleResArray[arrayIndx].rowIdsNum;k++)
										{
											int index=middleResArray[arrayIndx].rowIds[k];
											sum+=relationArray[relationIndex].columns[columnIndx].tuples[index].value;
											//temp->tuples[k].id=k;
										}

										//column=setColumnFromMiddleArray(middleResArray,relationIndex,columnIndx,j,relationArray);
										end = clock();
										cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
										//printf("SetCol Time = %f\n",cpu_time_used);
										//long sum=SumOneColumnRelation(column);
										//end = clock();
										//cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
										//printf("Sum Time = %f\n",cpu_time_used);
										/*Job_Scheduler* job_scheduler;
										job_scheduler=malloc(sizeof(Job_Scheduler));
										job_scheduler=initialize_scheduler(THREADS_NUM,NULL,NULL);
										Job *job;
										int segSize=middleResArray[arrayIndx].rowIdsNum/THREADS_NUM;
										for(int i=0;i<THREADS_NUM;i++){
											job=initializeJob("partition");
											job->partitionjob.start=i*segSize;
											job->partitionjob.end=job->partitionjob.start+segSize;
											job->partitionjob.rel='S';
											if(i==THREADS_NUM-1)
												job->partitionjob.end+=relS->num_of_tuples%THREADS_NUM;
											submit_Job(job_scheduler,job);
										}*/

										if(sum==0)
										{	//printf("NULL");
											write(STDOUT_FILENO,"NULL",strlen("NULL"));
											//strcpy(answer,"NULL");
										}
										else
										{	//printf("%ld",sum);
											char *str;
											str=malloc(100);
											sprintf(str,"%ld",sum);
											write(STDOUT_FILENO,str,strlen(str));
											memset(str,0,strlen(str)+1);
											/*if(!strcmp(answer,""))
											{
												sprintf(answer,"%ld",sum);
											}
											else
											{
												char *str;
												str=malloc(20);
												sprintf(str,"%ld",sum);
												write(STDOUT_FILENO,str,strlen(str)+1);
												strcat(answer,str);
												//memset(str,0,strlen(str));
											}*/

										}
										if(i!=data->numViewQuery-1)
										{
											//printf(" ");
											//strcat(answer," ");
											write(STDOUT_FILENO," ",strlen(" "));
										}
										//free(column->tuples);
										//free(column);
									}
								}
							}
							//printf("\n");
							//printf("answer =%s\n",answer);
							//write(STDOUT_FILENO,answer,strlen(answer)+1);
//							strcat(answer,"\n");
							write(STDOUT_FILENO,"\n",strlen("\n"));

						}
					}
					for(j=0;j<middleResultsCounter;j++){
						if(middleResArray[j].rowIdsNum!=0)
							free(middleResArray[j].rowIds);
						middleResArray[j].relation=-1;
						middleResArray[j].rowIdsNum=0;
						middleResArray[j].team=0;
						middleResArray[j].fromArray=-1;
						middleResArray[j].relation_id=-1;
					}
				}
				middleResultsCounter=0;
				//queryString=strtok(NULL,"\n");
		}
		//free(queryString);
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
		//bytes = read(STDIN_FILENO,buffer,sizeof(buffer));
	}
}
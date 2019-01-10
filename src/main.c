#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"
#include <unistd.h>
#include <string.h>
#include <stdbool.h>

int main(int argc,char** argv)
{

	char *filename,*paths,*onepath;
	FILE *fp;
	int fsize,i,j,k,pointer,number_of_files;
	multiColumnRelation *relationArray;
	all_stats *statsArray;
	if (argc!=2){
		printf("\n\nWRONG NUMBER OF ARGS!!!\n\n\n");
		return 1;
	}
	filename=malloc(strlen(argv[1])+strlen("small.init")+1);//dineis mono to arxeio , to path to ftiaxnw edw
	strcpy(filename,argv[1]);
	strcat(filename,"small.init");

	fp=fopen(filename,"r");

	fseek(fp,0,SEEK_END);
	fsize = ftell(fp);
	fseek(fp,0,SEEK_SET);
	paths=malloc(fsize+1);
	fread(paths,fsize,1,fp);
	fclose(fp);
	paths[fsize-1]='\0';
	//koitazw posa arxeia exw
	if(fsize==0){//na to dw pali
		printf("\n\nKENO ARXEIO !!!\n\n");
		return 1;
	}
	number_of_files=1;
	for(i=0;i<fsize;i++){
		if(paths[i]=='\n'){
			number_of_files++;
		}
	}
	//ftiaxnw ton pinaka relationArray kai pairnw ta data apo ka8e arxeio
	relationArray=malloc(number_of_files*sizeof(multiColumnRelation));

	statsArray=malloc(sizeof(all_stats));
	statsArray->rels=number_of_files;
	statsArray->cols=malloc(number_of_files*sizeof(uint64_t));
	statsArray->array_with_stats=malloc(number_of_files*sizeof(stats *));

	onepath=malloc(strlen(argv[1])+fsize+1);
	pointer=0;
	for(i=0;i<number_of_files;i++){
		memset(onepath,'\0',strlen(argv[1])+fsize+1);
		strcpy(onepath,argv[1]);
		j=strlen(argv[1]);
		while( paths[pointer]!='\n' && paths[pointer]!='\0' ){
			onepath[j]=paths[pointer];
			j++;
			pointer++;
		}
		pointer++;

		fp=fopen(onepath,"rb");
		uint64_t temp;
		fread(&temp,sizeof(uint64_t),1,fp);
		relationArray[i].rowCount=temp;
		fread(&temp,sizeof(uint64_t),1,fp);
		relationArray[i].colCount=temp;
		relationArray[i].columns = malloc((relationArray)[i].colCount*sizeof(oneColumnRelation));
		//relationArray[i].table=malloc(relationArray[i].colCount*sizeof(uint64_t *));
		//relationArray[i].stats=malloc(relationArray[i].colCount*sizeof(statistics));
		statsArray->cols[i]=relationArray[i].colCount;//
		statsArray->array_with_stats[i]=malloc(relationArray[i].colCount*sizeof(stats));//
		for(j=0;j<relationArray[i].colCount;j++){
			statsArray->array_with_stats[i][j].f=relationArray[i].rowCount;//
			//relationArray[i].table[j]=malloc(relationArray[i].rowCount*sizeof(uint64_t));//mporei na prepei na bgoun eksw
			relationArray[i].columns[j].tuples=malloc((relationArray)[i].rowCount*sizeof(tuple));//mporei na prepei na bgoun eksw

			for(k=0;k<relationArray[i].rowCount;k++){
				fread(&temp,sizeof(uint64_t),1,fp);
				//relationArray[i].table[j][k]=temp;
				(relationArray)[i].columns[j].tuples[k].value=temp;
				(relationArray)[i].columns[j].tuples[k].id=k;
//------------------------------//briskw min kai max gia to stats----------------------------
				if(k==0){
					statsArray->array_with_stats[i][j].l=temp;
					statsArray->array_with_stats[i][j].u=temp;
				}
				else{
					if(statsArray->array_with_stats[i][j].l>temp){
						statsArray->array_with_stats[i][j].l=temp;
					}

					if(statsArray->array_with_stats[i][j].u<temp){
						statsArray->array_with_stats[i][j].u=temp;
					}
				}
				//

			}
			//relationArray[i].stats[j].average=relationArray[i].stats[j].average/relationArray[i].rowCount;
			int d=0;
			if(statsArray->array_with_stats[i][j].u-statsArray->array_with_stats[i][j].l+1>500000){
				int sizeofbool=500000;
				bool distArray[sizeofbool];
				for(int qaz=0;qaz<sizeofbool;qaz++){
					distArray[qaz]=false;
				}

				/*for(k=0;k<relationArray[i].rowCount;k++){
					if(distArray[(relationArray[i].table[j][k]-statsArray->array_with_stats[i][j].l)%sizeofbool]==false){
						distArray[(relationArray[i].table[j][k]-statsArray->array_with_stats[i][j].l)%sizeofbool]=true;
						d++;
					}
				}*/
				for(k=0;k<(relationArray)[i].rowCount;k++){
					if(distArray[((relationArray)[i].columns[j].tuples[k].value-(statsArray)->array_with_stats[i][j].l)%sizeofbool]==false){
						distArray[((relationArray)[i].columns[j].tuples[k].value-(statsArray)->array_with_stats[i][j].l)%sizeofbool]=true;
						d++;
					}
				}
			}
			else{
				int sizeofbool=statsArray->array_with_stats[i][j].u-statsArray->array_with_stats[i][j].l+1;
				bool distArray[sizeofbool];
				for(int qaz=0;qaz<sizeofbool;qaz++){
					distArray[qaz]=false;
				}

				/*for(k=0;k<relationArray[i].rowCount;k++){
					if(distArray[relationArray[i].table[j][k]-statsArray->array_with_stats[i][j].l]==false){
						distArray[relationArray[i].table[j][k]-statsArray->array_with_stats[i][j].l]=true;
						d++;
					}
				}*/
				for(k=0;k<(relationArray)[i].rowCount;k++){
					if(distArray[(relationArray)[i].columns[j].tuples[k].value-(statsArray)->array_with_stats[i][j].l]==false){
						distArray[(relationArray)[i].columns[j].tuples[k].value-(statsArray)->array_with_stats[i][j].l]=true;
						d++;
					}
				}
			}
			statsArray->array_with_stats[i][j].d=d;

			//printf("stats %ld %ld %ld %ld\n",statsArray->array_with_stats[i][j].l,statsArray->array_with_stats[i][j].u,statsArray->array_with_stats[i][j].f,statsArray->array_with_stats[i][j].d);
			//printf("check %ld %ld %d\n",relationArray[i].stats[j].min,relationArray[i].stats[j].max,relationArray[i].rowCount);
					}
					fclose(fp);
	}
	free(filename);
	free(onepath);
	free(paths);

	filename=malloc(strlen(argv[1])+strlen("small.work")+1);//dineis mono to arxeio , to path to ftiaxnw edw
	strcpy(filename,argv[1]);
	strcat(filename,"small.work");
	readWorkFile(filename,relationArray,statsArray);
	free(filename);
	/*for(i=0;i<number_of_files;i++){
		for(j=0;j<relationArray[i].colCount;j++){
			free(relationArray[i].table[j]);
		}
		free(relationArray[i].table);
	}
	free(relationArray);*/
}


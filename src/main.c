#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"
#include <unistd.h>
#include <string.h>

int main(int argc,char** argv)
{

	char *filename,*paths,*onepath;
	FILE *fp;
	int fsize,i,j,k,pointer,number_of_files;
	multiColumnRelation *relationArray;
	if (argc!=3){
		printf("\n\nWRONG NUMBER OF ARGS!!!\n\n\n");
		return 1;
	}
	filename=malloc(strlen("input-files/")+strlen(argv[1])+1);//dineis mono to arxeio , to path to ftiaxnw edw
	strcpy(filename,"input-files/");
	strcat(filename,argv[1]);

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

	onepath=malloc(strlen("input-files/")+fsize+1);
	pointer=0;
	for(i=0;i<number_of_files;i++){
		memset(onepath,'\0',fsize);
		strcpy(onepath,"input-files/");
		j=strlen("input-files/");
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
		relationArray[i].table=malloc(relationArray[i].colCount*sizeof(uint64_t *));
		relationArray[i].stats=malloc(relationArray[i].colCount*sizeof(statistics));
		for(j=0;j<relationArray[i].colCount;j++){
			relationArray[i].table[j]=malloc(relationArray[i].rowCount*sizeof(uint64_t));//mporei na prepei na bgoun eksw
			for(k=0;k<relationArray[i].rowCount;k++){
				fread(&temp,sizeof(uint64_t),1,fp);
				relationArray[i].table[j][k]=temp;

				//briskw min kai max
				if(k==0){
					relationArray[i].stats[j].min=temp;
					relationArray[i].stats[j].max=temp;
				}
				else{
					if(relationArray[i].stats[j].min>temp){
						relationArray[i].stats[j].min=temp;
					}

					if(relationArray[i].stats[j].max<temp){
						relationArray[i].stats[j].max=temp;
					}
				}

				//briskw average
				if(k==0){
					relationArray[i].stats[j].average=temp;
				}
				else{
					relationArray[i].stats[j].average+=temp;
				}
			}
			relationArray[i].stats[j].average=relationArray[i].stats[j].average/relationArray[i].rowCount;
		}

		fclose(fp);
	}
	free(filename);
	free(onepath);
	free(paths);
	readWorkFile(argv[2],relationArray);
	for(i=0;i<number_of_files;i++){
		for(j=0;j<relationArray[i].colCount;j++){
			free(relationArray[i].table[j]);
		}
		free(relationArray[i].table);
	}
	free(relationArray);
}


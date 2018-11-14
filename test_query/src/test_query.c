/*
 ============================================================================
 Name        : test_query.c
 Author      : takis
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define bufSize 1024

void addQueryData(char *token,int part){
	char *temp;
	if(part==0){
		printf("rel: ");
		temp=strtok(token," ");
		while(temp!=NULL){
			printf("%s ",temp);
			temp=strtok(NULL," ");
		}
	}
	else if(part==1){
		printf("Pred: ");
		temp=strtok(token,"&");
		while(temp!=NULL){
			printf("%s ",temp);
			temp=strtok(NULL,"&");
		}
	}
	else if(part==2){
		printf("Views: ");
		temp=strtok(token,"&");
		while(temp!=NULL){
			printf("%s ",temp);
			temp=strtok(NULL," ");
		}
	}
	printf("\n");
}

int main(int argc, char *argv[])
{
  FILE* fp;
  int part;
  char buf[bufSize];
  if (argc!=2){
    fprintf(stderr,"Usage: %s <soure-file>\n", argv[0]);
    return 1;
  }
  if ((fp = fopen(argv[1], "r")) == NULL){
    perror("fopen source-file");
    return 1;
  }
  char *temp,*token;
  char *token1,*token2,*token3;
  while (fgets(buf, sizeof(buf), fp) != NULL){
    buf[strlen(buf)-1]='\0';
    temp=buf;

    token=strtok(temp,"|");
    if(strcmp(token,"F")==0)
    	continue;
    token1=token;
    token2=strtok(NULL,"|");
    token3=strtok(NULL,"|");

    //printf("%s | %s | %s\n",token1,token2,token3);
    addQueryData(token1,0);
    addQueryData(token2,1);
    addQueryData(token3,2);
    printf("\n");
  }

  fclose(fp);
  return 0;
}

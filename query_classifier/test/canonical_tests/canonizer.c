#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <query_classifier.h>
#include <buffer.h>
#include <mysql.h>

static char* server_options[] = {
        "SkySQL Gateway",
	"--datadir=./",
	"--language=./",
	"--skip-innodb",
	"--default-storage-engine=myisam",
	NULL
};

const int num_elements = (sizeof(server_options) / sizeof(char *)) - 1;

static char* server_groups[] = {
        "embedded",
        "server",
        "server",
        NULL
};

int main(int argc, char** argv)
{

  int fdin,fdout,i=0,fnamelen,lines = 0;
  unsigned int psize,tptr = 0,fsz = 0;
  GWBUF** qbuff;
  char *qin, *outnm, *buffer, *tok;
  
  if(argc != 3){
    printf("Usage: canonizer <input file> <output file>\n");
    return 1;
  } 
  
  bool failed = mysql_library_init(num_elements, server_options, server_groups);

  if(failed){
    printf("Embedded server init failed.\n");
    return 1;
  }

  fnamelen = strlen(argv[1]) + 16;
  fdin = open(argv[1],O_RDONLY);
  fsz = lseek(fdin,0,SEEK_END);
  lseek(fdin,0,SEEK_SET);

  if((buffer = malloc(sizeof(char)*fsz)) == NULL){
    printf("Error: Failed to allocate memory.");
    return 1;
  }

  read(fdin,buffer,fsz);
  tok = buffer;
  
  while(tptr < fsz){
    if(*(tok + tptr++) == '\n'){
      lines++;
    }    
  }
  
  qbuff = malloc(sizeof(GWBUF*)*lines);
  
  i = 0;
  tptr = 0;
  tok = buffer;

  while(((tok + tptr) - buffer) < fsz){

    if(*(tok + tptr++) == '\n' || ((tok + tptr) - buffer) >= fsz){

      qbuff[i] = gwbuf_alloc(tptr + 5);

      if(qbuff[i]){

      *(qbuff[i]->sbuf->data + 0) = (unsigned char)tptr;
      *(qbuff[i]->sbuf->data + 1) = (unsigned char)(tptr>>8);
      *(qbuff[i]->sbuf->data + 2) = (unsigned char)(tptr>>16);
      *(qbuff[i]->sbuf->data + 3) = 0x00;
      *(qbuff[i]->sbuf->data + 4) = 0x03;
      memcpy(qbuff[i]->sbuf->data + 5,tok,tptr - 1);
      *(qbuff[i]->sbuf->data + 5 + tptr - 1) = 0x00;
      i++;
      tok += tptr;
      tptr = 0;
      }else{
	printf("Error: cannot allocate new GWBUF.");
      }


    }
  }

  fdout = open(argv[2],O_TRUNC|O_CREAT|O_WRONLY,S_IRWXU|S_IXGRP|S_IXOTH);

  for(i = 0;i<lines;i++){
    parse_query(qbuff[i]);
    tok = skygw_get_canonical(qbuff[i]);
    write(fdout,tok,strlen(tok));
    write(fdout,"\n",1);
    free(tok);
    gwbuf_free(qbuff[i]);
  }
  mysql_library_end();
  close(fdin);
  close(fdout);
  
  return 0;
}

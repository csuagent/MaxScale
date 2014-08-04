#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ini.h>
#include <stdint.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <mysql.h>

typedef struct delivery_t
{
  uint64_t dtag;
  amqp_message_t* message;
  struct delivery_t *next,*prev;
}DELIVERY;

typedef struct consumer_t
{
  char *hostname,*vhost,*user,*passwd,*queue,*dbserver,*dbname,*dbuser,*dbpasswd;
  DELIVERY* query_stack;
  int port,dbport;
}CONSUMER;

static CONSUMER* c_inst;

int handler(void* user, const char* section, const char* name,
	    const char* value)
{
  if(strcmp(section,"consumer") == 0){
    
    if(strcmp(name,"hostname") == 0){
      c_inst->hostname = strdup(value);
    }else if(strcmp(name,"vhost") == 0){
      c_inst->vhost = strdup(value);
    }else if(strcmp(name,"port") == 0){
      c_inst->port = atoi(value);
    }else if(strcmp(name,"user") == 0){
      c_inst->user = strdup(value);
    }else if(strcmp(name,"passwd") == 0){
      c_inst->passwd = strdup(value);
    }else if(strcmp(name,"queue") == 0){
      c_inst->queue = strdup(value);
    }else if(strcmp(name,"dbserver") == 0){
      c_inst->dbserver = strdup(value);
    }else if(strcmp(name,"dbport") == 0){
      c_inst->dbport = atoi(value);
    }else if(strcmp(name,"dbname") == 0){
      c_inst->dbname = strdup(value);
    }else if(strcmp(name,"dbuser") == 0){
      c_inst->dbuser = strdup(value);
    }else if(strcmp(name,"dbpasswd") == 0){
      c_inst->dbpasswd = strdup(value);
    }

  }

  return 1;
}
int isPair(amqp_message_t* a, amqp_message_t* b)
{
  int keylen = a->properties.correlation_id.len >=
    b->properties.correlation_id.len ?
    a->properties.correlation_id.len :
    b->properties.correlation_id.len;
  
  return strncmp(a->properties.correlation_id.bytes,
		 b->properties.correlation_id.bytes,
		 keylen) == 0 ? 1 : 0;
}

int connectToServer(MYSQL* server)
{

  
  mysql_init(server);
  mysql_options(server,MYSQL_READ_DEFAULT_GROUP,"client");
  mysql_options(server,MYSQL_OPT_GUESS_CONNECTION,0);

  MYSQL* result =  mysql_real_connect(server,
				      "127.0.0.1",
				      "maxuser",
				      "maxpwd",
				      NULL,
				      4006,
				      NULL,
				      0);
 
  
  if(result==NULL){
    printf("Error: Could not connect to MySQL sever: %s\n",mysql_error(server));
    return 0;
  }

  
  char *qstr = calloc(1024,sizeof(char));
  int bsz = 1024;
  
  if(!qstr){
    printf( "Fatal Error: Cannot allocate enough memory.\n");
    return 0;
  }


  /**Connection ok, check that the database and table exist*/

  MYSQL_RES *res;
  res = mysql_list_dbs(server,c_inst->dbname);
  if(!mysql_fetch_row(res)){

    memset(qstr,0,bsz);
    sprintf(qstr,"CREATE DATABASE %s;",c_inst->dbname);
    mysql_query(server,qstr);  
    memset(qstr,0,bsz);
    sprintf(qstr,"USE %s;",c_inst->dbname);
    mysql_query(server,qstr);  
    memset(qstr,0,bsz);
    sprintf(qstr,"CREATE TABLE pairs (query VARCHAR(2048), reply VARCHAR(2048), tag VARCHAR(64));");
    mysql_query(server,qstr);
    mysql_free_result(res);
 
  }else{

    mysql_free_result(res);

    memset(qstr,0,bsz);
    sprintf(qstr,"USE %s;",c_inst->dbname);
    mysql_query(server,qstr);  

    memset(qstr,0,bsz);
    sprintf(qstr,"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'; ",
	    c_inst->dbname, "pairs");
    if(mysql_query(server,qstr)){
      printf("Error: Could not send query MySQL sever: %s\n",mysql_error(server));
    }
    res = mysql_store_result(server);
    if(!mysql_fetch_row(res)){

      memset(qstr,0,bsz);
      sprintf(qstr,"CREATE TABLE pairs (query VARCHAR(2048), reply VARCHAR(2048), tag VARCHAR(64));");
      mysql_query(server,qstr);
    }

  }

  free(qstr);

  return 1;
}

int sendToServer(MYSQL* server, amqp_message_t* a, amqp_message_t* b){

  amqp_message_t *msg, *reply;
  char *qstr = calloc(2048,sizeof(char));

  if(!qstr){
    printf( "Fatal Error: Cannot allocate enough memory.\n");
    return 0;
  }

  if( a->properties.message_id.len == strlen("query") &&
      strncmp(a->properties.message_id.bytes,"query",
	      a->properties.message_id.len) == 0){

    msg = a;
    reply = b;

  }else{

    msg = b;
    reply = a;

  }
  
  sprintf(qstr,"INSERT INTO pairs VALUES ('%.*s','%.*s','%.*s');",
	  (int)msg->body.len,
	  (char *)msg->body.bytes,
	  (int)reply->body.len,
	  (char *)reply->body.bytes,
	  (int)msg->properties.correlation_id.len,
	  (char *)msg->properties.correlation_id.bytes);
  
  
   if(mysql_query(server,qstr)){
      printf("Could not send query to SQL server.\n");
      free(qstr);
      return 0;
    }
  

  printf("pair: %.*s\nquery: %.*s\nreply: %.*s\n",
	 (int)msg->properties.correlation_id.len,
	 (char *)msg->properties.correlation_id.bytes,
	 (int)msg->body.len,
	 (char *)msg->body.bytes,
	 (int)reply->body.len,
	 (char *)reply->body.bytes);
  return 1;
}
int main(int argc, char** argv)
{
  const char* fname = "consumer.cnf";
  int channel = 1, all_ok = 1, status = AMQP_STATUS_OK;
  amqp_socket_t *socket = NULL;
  amqp_connection_state_t conn;
  amqp_rpc_reply_t ret;
  amqp_message_t *reply = NULL;
  amqp_frame_t frame;
  struct timeval timeout;
  MYSQL db_inst;

  static char* options[] = {
    "consumer",
    "--no-defaults",
    "--datadir=/tmp",
    "--language=/home/markus/MaxScale_fork/rabbitmq_consumer/english",
    "--skip-innodb",
    "--default-storage-engine=myisam",
    NULL
  };

  static char* groups[] = {"embedded","client",NULL};
  int num_elem = (sizeof(options) / sizeof(char *)) - 1;
  timeout.tv_sec = 2;
  timeout.tv_usec = 0;


  if((c_inst = calloc(1,sizeof(CONSUMER))) == NULL){
    printf( "Fatal Error: Cannot allocate enough memory.\n");
    return 1;
  }

  /**Parse the INI file*/
  if(ini_parse(fname,handler,NULL) < 0){
    printf( "Fatal Error: Error parsing configuration file!\n");
    goto fatal_error;
  }
  

  /**Confirm that all parameters were in the configuration file*/
  if(!c_inst->hostname||!c_inst->vhost||!c_inst->user||
     !c_inst->passwd||!c_inst->dbpasswd||!c_inst->queue||
     !c_inst->dbserver||!c_inst->dbname||!c_inst->dbuser){
    printf( "Fatal Error: Inadequate configuration file!\n");
    goto fatal_error;    
  }

  mysql_library_init(num_elem, options, groups);
  connectToServer(&db_inst);

  if((conn = amqp_new_connection()) == NULL || 
     (socket = amqp_tcp_socket_new(conn)) == NULL){
    printf( "Fatal Error: Cannot create connection object or socket.\n");
    goto fatal_error;
  }
  
  if(amqp_socket_open(socket, c_inst->hostname, c_inst->port)){
    printf( "Error: Cannot open socket.\n");
    goto error;
  }
  
  ret = amqp_login(conn, c_inst->vhost, 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, c_inst->user, c_inst->passwd);

  if(ret.reply_type != AMQP_RESPONSE_NORMAL){
    printf( "Error: Cannot login to server.\n");
    goto error;
  }

  amqp_channel_open(conn, channel);
  ret = amqp_get_rpc_reply(conn);

  if(ret.reply_type != AMQP_RESPONSE_NORMAL){
    printf( "Error: Cannot open channel.\n");
    goto error;
  }  

  reply = malloc(sizeof(amqp_message_t));
  if(!reply){
    printf( "Error: Cannot allocate enough memory.\n");
    goto error;
  }
  amqp_basic_consume(conn,channel,amqp_cstring_bytes(c_inst->queue),amqp_empty_bytes,0,0,0,amqp_empty_table);

  while(all_ok){
     
    status = amqp_simple_wait_frame_noblock(conn,&frame,&timeout);

    /**No frames to read from server, possibly out of messages*/
    if(status == AMQP_STATUS_TIMEOUT){ 
      printf("Frame wait timed out, trying again in %d seconds.\n",((int)timeout.tv_sec)*2);	
      sleep(timeout.tv_sec);
      if(timeout.tv_sec < 128){
	timeout.tv_sec *= 2;
      }
      
      continue;
    }else{
      timeout.tv_sec = 2;
    }

    if(frame.payload.method.id == AMQP_BASIC_DELIVER_METHOD){

      amqp_basic_deliver_t* decoded = (amqp_basic_deliver_t*)frame.payload.method.decoded;
	
      amqp_read_message(conn,channel,reply,0);

      if(reply->properties.message_id.len > 0 &&
	 strncmp(reply->properties.message_id.bytes,
		 "query",reply->properties.message_id.len) == 0)
	{

	  /**Found a query, store it*/
	  DELIVERY* dlvr = calloc(1,sizeof(DELIVERY));

	  if(dlvr){

	    dlvr->dtag = decoded->delivery_tag;
	    dlvr->message = reply;
	    dlvr->next = c_inst->query_stack;
	    if(c_inst->query_stack){
	      c_inst->query_stack->prev = dlvr;
	    }
	    c_inst->query_stack = dlvr;
	    reply = malloc(sizeof(amqp_message_t));
	      
	  }

	}else if(reply->properties.message_id.len > 0 &&
		 strncmp(reply->properties.message_id.bytes,
			 "reply",reply->properties.message_id.len) == 0){

	/**Found a reply, try to pair it*/
	DELIVERY* dlvr = c_inst->query_stack;
	int was_paired = 0;
	while(dlvr){
	  if(isPair(dlvr->message,reply)){

	    sendToServer(&db_inst, dlvr->message, reply);
	    amqp_basic_ack(conn,channel,decoded->delivery_tag,0);
	    amqp_basic_ack(conn,channel,dlvr->dtag,0);
	    amqp_destroy_message(dlvr->message);
	    amqp_destroy_message(reply);

	    if(dlvr->next){
	      dlvr->next->prev = dlvr->prev;
	    }
	    if(dlvr->prev){
	      dlvr->prev->next = dlvr->next;
	    }

	    if(dlvr == c_inst->query_stack){
	      if(dlvr->next == NULL){
		c_inst->query_stack = NULL;
	      }else{
		c_inst->query_stack = dlvr->next;
	      }
	      
	    }

	    free(dlvr->message);
	    free(dlvr);
	    was_paired = 1;
	    dlvr = NULL;
	  }else{
	    dlvr = dlvr->next;
	  }
	}
	if(!was_paired){
	  amqp_basic_reject(conn,channel,decoded->delivery_tag,1);
	  amqp_destroy_message(reply);
	}
	  
      }else{ /**Something neither a query or a reply received, send it back*/
	amqp_destroy_message(reply);
	amqp_basic_reject(conn,channel,decoded->delivery_tag,1);
      }
      
    }else{
      printf("Received method from server: %s\n",amqp_method_name(frame.payload.method.id));
      all_ok = 0;
      goto error;
    }

  }


 error:

  mysql_close(&db_inst);
  mysql_library_end();
  if(c_inst && c_inst->query_stack){

    while(c_inst->query_stack){
      DELIVERY* d = c_inst->query_stack->next;
      amqp_destroy_message(c_inst->query_stack->message);
      free(c_inst->query_stack);
      c_inst->query_stack = d;
    }

  }
  
  amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
  amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(conn);
 fatal_error:
  if(c_inst){

    free(c_inst->hostname);
    free(c_inst->queue);
    free(c_inst->dbserver);
    free(c_inst->dbname);
    free(c_inst->dbuser);
    free(c_inst->dbpasswd);    
    free(c_inst);
    
  }

  
  
  return all_ok;
}

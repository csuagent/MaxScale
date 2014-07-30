#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <stdint.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

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
int sendToServer(amqp_message_t* a, amqp_message_t* b){

  amqp_message_t *msg, *reply;

  if( a->properties.message_id.len == strlen("query") &&
      strncmp(a->properties.message_id.bytes,"query",
	      a->properties.message_id.len) == 0){

    msg = a;
    reply = b;

  }else{

    msg = b;
    reply = a;

  }

  printf("pair: %.*s\nquery: %.*s\nreply: %.*s",
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
  char *hostname,*queue;
  int port, channel = 1, all_ok = 1, have_q = 0, have_r = 0, status = AMQP_STATUS_OK;
  amqp_socket_t *socket = NULL;
  amqp_connection_state_t conn;
  amqp_rpc_reply_t ret;
  amqp_message_t *query = NULL,*reply = NULL;
  amqp_frame_t frame;
  struct timeval timeout;

  if (argc < 4) {
    fprintf(stderr, "Usage: consumer host port queue\n");
    return 1;
  }
  hostname = strdup(argv[1]);
  port = atoi(argv[2]);
  queue = strdup(argv[3]);
  timeout.tv_sec = 5;
  timeout.tv_usec = 0;

  if((conn = amqp_new_connection()) == NULL || 
     (socket = amqp_tcp_socket_new(conn)) == NULL){
    printf("Fatal Error: Cannot create connection object or socket.");
    goto fatal_error;
  }
  
  if(amqp_socket_open(socket, hostname, port)){
    printf("Error: Cannot open socket.");
    goto error;
  }
  
  ret = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");

  if(ret.reply_type != AMQP_RESPONSE_NORMAL){
    printf("Error: Cannot login to server.\n");
    goto error;
  }

  amqp_channel_open(conn, channel);
  ret = amqp_get_rpc_reply(conn);

  if(ret.reply_type != AMQP_RESPONSE_NORMAL){
    printf("Error: Cannot open channel.\n");
    goto error;
  }  

  query = malloc(sizeof(amqp_message_t));
  reply = malloc(sizeof(amqp_message_t));
  if(!query || !reply){
    printf("Error: Cannot allocate enough memory.");
    goto error;
  }
  amqp_basic_consume(conn,channel,amqp_cstring_bytes(queue),amqp_empty_bytes,0,0,0,amqp_empty_table);

  while(all_ok){
      
    if(!have_q){ /**Get a query*/
     
      status = amqp_simple_wait_frame_noblock(conn,&frame,&timeout);
      
      /**No frames to read from server, possibly out of messages*/
      if(status == AMQP_STATUS_TIMEOUT){ 
	sleep(timeout.tv_sec);
	continue;
      }

      if(frame.payload.method.id == AMQP_BASIC_DELIVER_METHOD){

	amqp_basic_deliver_t* decoded = (amqp_basic_deliver_t*)frame.payload.method.decoded;
	query = malloc(sizeof(amqp_message_t));

	amqp_read_message(conn,channel,query,0);
	if(query->properties.message_id.len > 0 &&
	   strncmp(query->properties.message_id.bytes,
		   "query",query->properties.message_id.len) == 0)
	  {
	    amqp_basic_ack(conn,channel,decoded->delivery_tag,0);
	    have_q = 1;
	  }else{
	  amqp_basic_reject(conn,channel,decoded->delivery_tag,1);
	}
      }
    
    }else if (!have_r){ /**Check for a reply*/
      
      status = amqp_simple_wait_frame_noblock(conn,&frame,&timeout);      

      /**No frames to read from server, possibly out of messages*/
      if(status == AMQP_STATUS_TIMEOUT){ 
	sleep(timeout.tv_sec);
	continue;
      }
	
      if(frame.payload.method.id == AMQP_BASIC_DELIVER_METHOD){

	amqp_basic_deliver_t* decoded = (amqp_basic_deliver_t*)frame.payload.method.decoded;

	amqp_read_message(conn,channel,reply,0);
	if(reply->properties.message_id.len > 0 &&
	   strncmp(reply->properties.message_id.bytes,
		   "reply",reply->properties.message_id.len) == 0 && 
	   isPair(query,reply))
	  {
	    amqp_basic_ack(conn,channel,decoded->delivery_tag,0);
	    have_r = 1;
	  }else{
	  amqp_basic_reject(conn,channel,decoded->delivery_tag,1);
	}
    
      }

    }else if( have_q && have_r){ /**Pair formed, send to server*/
 
      sendToServer(query,reply);
      have_q = have_r = 0;

    }


  }
  return 0;

 error:
  amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS);
  amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(conn);
 fatal_error:
  free(hostname);
  free(queue);
  return 1;
}

/*
 * This file contains the implementation for the dome generic event Kafka writer.
 *
 ***/

#include <string>
#include <sstream>
#include <iostream>
#include <stdlib.h>
/* Include the Kafka library instead of MySQL.
   Eventually, include Avro too. */
#include <librdkafka/rdkafka.h>
#include <syslog.h>
#include "SAL_dome.h"
#include "SAL_actors.h"
#include "ccpp_sal_dome.h"
#include "os.h"
using namespace DDS;
using namespace dome;

/* Three static variables for Kafka */
static rd_kafka_t *rk;
static int quiet=0;
static int run=1;

/* entry point exported and demangled so symbol can be found in shared library */
extern "C"
{
  OS_API_EXPORT
  int test_dome_event_kafkawriter();
}

/**
 * Message delivery report callback using the richer rd_kafka_message_t object.
 */
static void msg_delivered (rd_kafka_t *rk,
                           const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
    else if (!quiet)
        fprintf(stderr,
                        "%% Message delivered (%zd bytes, offset %" PRId64", "
                        "partition %" PRId32"): %.*s\n",
                        rkmessage->len, rkmessage->offset,
            rkmessage->partition,
            (int)rkmessage->len, (const char *)rkmessage->payload);
}

/**
 * Kafka logger callback (optional)
 */

/* Not currently using, but keep around for now.

static void logger (const rd_kafka_t *rk, int level,
            const char *fac, const char *buf) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
        (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
        level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

*/

int test_dome_event_kafkawriter()
{

  char *thequery = (char *) malloc(sizeof(char)*100000);
  SAL_dome mgr = SAL_dome();


  os_time delay_10us = { 0, 10000 };
  int numsamp = 0;
  int actorIdx = 0;
  int isyslog = 0;
  int iloop = 0;
  int mstatus = 0;
  int status=0;
  mgr.salTelemetrySub("dome_logevent");
  actorIdx = SAL__dome_logevent_ACTOR;
  DataReader_var logevent_dreader = mgr.getReader(actorIdx);
  dome::logeventDataReader_var logevent_SALReader = dome::logeventDataReader::_narrow(logevent_dreader.in());
  mgr.checkHandle(logevent_SALReader.in(), "dome::logeventDataReader::_narrow");

  mgr.salTelemetrySub("dome_logevent_appliedSettingsMatchStart");
  actorIdx = SAL__dome_logevent_appliedSettingsMatchStart_ACTOR;
  DataReader_var logevent_appliedSettingsMatchStart_dreader = mgr.getReader(actorIdx);
  dome::logevent_appliedSettingsMatchStartDataReader_var logevent_appliedSettingsMatchStart_SALReader = dome::logevent_appliedSettingsMatchStartDataReader::_narrow(logevent_appliedSettingsMatchStart_dreader.in());
  mgr.checkHandle(logevent_appliedSettingsMatchStart_SALReader.in(), "dome::logevent_appliedSettingsMatchStartDataReader::_narrow");

  mgr.salTelemetrySub("dome_logevent_detailedState");
  actorIdx = SAL__dome_logevent_detailedState_ACTOR;
  DataReader_var logevent_detailedState_dreader = mgr.getReader(actorIdx);
  dome::logevent_detailedStateDataReader_var logevent_detailedState_SALReader = dome::logevent_detailedStateDataReader::_narrow(logevent_detailedState_dreader.in());
  mgr.checkHandle(logevent_detailedState_SALReader.in(), "dome::logevent_detailedStateDataReader::_narrow");

  mgr.salTelemetrySub("dome_logevent_errorCode");
  actorIdx = SAL__dome_logevent_errorCode_ACTOR;
  DataReader_var logevent_errorCode_dreader = mgr.getReader(actorIdx);
  dome::logevent_errorCodeDataReader_var logevent_errorCode_SALReader = dome::logevent_errorCodeDataReader::_narrow(logevent_errorCode_dreader.in());
  mgr.checkHandle(logevent_errorCode_SALReader.in(), "dome::logevent_errorCodeDataReader::_narrow");

  mgr.salTelemetrySub("dome_logevent_settingVersions");
  actorIdx = SAL__dome_logevent_settingVersions_ACTOR;
  DataReader_var logevent_settingVersions_dreader = mgr.getReader(actorIdx);
  dome::logevent_settingVersionsDataReader_var logevent_settingVersions_SALReader = dome::logevent_settingVersionsDataReader::_narrow(logevent_settingVersions_dreader.in());
  mgr.checkHandle(logevent_settingVersions_SALReader.in(), "dome::logevent_settingVersionsDataReader::_narrow");

  mgr.salTelemetrySub("dome_logevent_stateChanged");
  actorIdx = SAL__dome_logevent_stateChanged_ACTOR;
  DataReader_var logevent_stateChanged_dreader = mgr.getReader(actorIdx);
  dome::logevent_stateChangedDataReader_var logevent_stateChanged_SALReader = dome::logevent_stateChangedDataReader::_narrow(logevent_stateChanged_dreader.in());
  mgr.checkHandle(logevent_stateChanged_SALReader.in(), "dome::logevent_stateChangedDataReader::_narrow");

  mgr.salTelemetrySub("dome_logevent_summaryState");
  actorIdx = SAL__dome_logevent_summaryState_ACTOR;
  DataReader_var logevent_summaryState_dreader = mgr.getReader(actorIdx);
  dome::logevent_summaryStateDataReader_var logevent_summaryState_SALReader = dome::logevent_summaryStateDataReader::_narrow(logevent_summaryState_dreader.in());
  mgr.checkHandle(logevent_summaryState_SALReader.in(), "dome::logevent_summaryStateDataReader::_narrow");


  /* Kafka setup; replaces MySQL setup */
  
  char *brokers;
  rd_kafka_topic_t *rkt;
  char mode = 'C';
  const char *topic = "dome_event"; /* Substitute from template */
  const char *estr; /* Kafka error string for legacy methods */
  int partition = RD_KAFKA_PARTITION_UA;
  int opt;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;
  char errstr[512];
  int64_t start_offset = 0;
  int do_conf_dump = 0;
  char tmp[16];
  int64_t seek_offset = 0;
  int64_t tmp_offset = 0;
  int get_wmarks = 0;
  rd_kafka_headers_t *hdrs = NULL;
  rd_kafka_resp_err_t err;

  /* Kafka configuration */
  conf = rd_kafka_conf_new();

  /* Set up a message delivery report callback.
   * It will be called once for each message, either on successful
   * delivery to broker, or upon failure to deliver to broker. */
  rd_kafka_conf_set_dr_msg_cb(conf, msg_delivered);

  /* Create Kafka handle */
  if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                          errstr, sizeof(errstr)))) {
      fprintf(stderr,
              "%% Failed to create new producer: %s\n",
              errstr);
      exit(1);
  }

  /* New environment variable for Kafka endpoints */
  /* Comma-separated list of host:port tuples */
  /* e.g. "172.17.0.5:9092,172.17.0.6:9092" */
  brokers = getenv("LSST_KAFKA_BROKERS");
  if (brokers == NULL) {
      fprintf(stderr,"KAFKA : LSST_KAFKA_BROKERS not defined\n");
      exit(1);
  }


  /* Add brokers */
  if (rd_kafka_brokers_add(rk, brokers) == 0) {
      fprintf(stderr, "%% No valid brokers specified\n");
      exit(1);
  }

  /* Quick termination */
  snprintf(tmp, sizeof(tmp), "%i", SIGIO);
  rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);

  /* Topic configuration */
  topic_conf = rd_kafka_topic_conf_new();
  if (topic_conf == NULL) {
    err = rd_kafka_last_error();
    estr = rd_kafka_err2str(err);
    fprintf(stderr,"%% Failed to create Kafka topic config: %s.\n", estr);
    exit(1);
  }

  /* Create topic */
  rkt = rd_kafka_topic_new(rk, topic, topic_conf);
 if (rkt == NULL) {
    err = rd_kafka_last_error();
    estr = rd_kafka_err2str(err);
    fprintf(stderr,"%% Failed to create Kafka topic: %s.\n", estr);
    exit(1);
  }
 topic_conf = NULL; /* Now owned by topic */

  cout << "Kafka client for dome ready" << endl;

  int sendcnt=0;
  size_t len;
  /* Inside the main loop, we've changed the formatting of the 'thequery'
     assembly a little, and replaced the mysql call with a call to Kafka
     rd_kafka_produce() */
  while (1) {


       dome::logevent_appliedSettingsMatchStartSeq myData_logevent_appliedSettingsMatchStart;
       SampleInfoSeq_var logevent_appliedSettingsMatchStart_info = new SampleInfoSeq;
       status = logevent_appliedSettingsMatchStart_SALReader->take(myData_logevent_appliedSettingsMatchStart, logevent_appliedSettingsMatchStart_info, 100, ANY_SAMPLE_STATE, ANY_VIEW_STATE, ANY_INSTANCE_STATE);
       mgr.checkStatus(status,"dome::logevent_appliedSettingsMatchStartDataReader::take");
       numsamp = myData_logevent_appliedSettingsMatchStart.length();
       if (status == SAL__OK && numsamp > 0) {
        for (iloop=0;iloop<numsamp;iloop++) {
         if (myData_logevent_appliedSettingsMatchStart[iloop].private_origin != 0) {
          myData_logevent_appliedSettingsMatchStart[iloop].private_rcvStamp = mgr.getCurrentTime();

          /* Modified query for Kafka */
          /* Eventually, produce a structured Apache Avro object instead. */
          sprintf(thequery,"dome_event dome_logevent_appliedSettingsMatchStart:('%s', %lf , %lf , %d , %d , %d , %d , %d)"  , myData_logevent_appliedSettingsMatchStart[iloop].private_revCode.m_ptr , myData_logevent_appliedSettingsMatchStart[iloop].private_sndStamp , myData_logevent_appliedSettingsMatchStart[iloop].private_rcvStamp , myData_logevent_appliedSettingsMatchStart[iloop].private_seqNum , myData_logevent_appliedSettingsMatchStart[iloop].private_origin , myData_logevent_appliedSettingsMatchStart[iloop].private_host , myData_logevent_appliedSettingsMatchStart[iloop].appliedSettingsMatchStartIsTrue , myData_logevent_appliedSettingsMatchStart[iloop].priority );
          cout << thequery << endl;

          /* Kafka boilerplate to submit query */
          err = RD_KAFKA_RESP_ERR_NO_ERROR;
	  len = strlen(thequery);
          if (rd_kafka_produce(rkt, partition,
                               RD_KAFKA_MSG_F_COPY,
                               thequery,
                               len,
                               /* No key, no opaque */
                               NULL, 0, NULL) == -1) {
              err = rd_kafka_last_error();
          }
          if (err) {
              fprintf(stderr,
                      "%% Failed to produce to topic %s "
                      "partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(err));

              /* Poll to handle delivery reports */
              rd_kafka_poll(rk, 0);
              continue;
          }
          if (!quiet)
              fprintf(stderr, "%% Sent %zd bytes to topic "
                      "%s partition %i\n",
                      len, rd_kafka_topic_name(rkt), partition);
          sendcnt++;
          /* Poll to handle delivery reports */
          rd_kafka_poll(rk, 0);
         }
        }
       }
       status = logevent_appliedSettingsMatchStart_SALReader->return_loan(myData_logevent_appliedSettingsMatchStart, logevent_appliedSettingsMatchStart_info);


       dome::logevent_detailedStateSeq myData_logevent_detailedState;
       SampleInfoSeq_var logevent_detailedState_info = new SampleInfoSeq;
       status = logevent_detailedState_SALReader->take(myData_logevent_detailedState, logevent_detailedState_info, 100, ANY_SAMPLE_STATE, ANY_VIEW_STATE, ANY_INSTANCE_STATE);
       mgr.checkStatus(status,"dome::logevent_detailedStateDataReader::take");
       numsamp = myData_logevent_detailedState.length();
       if (status == SAL__OK && numsamp > 0) {
        for (iloop=0;iloop<numsamp;iloop++) {
         if (myData_logevent_detailedState[iloop].private_origin != 0) {
          myData_logevent_detailedState[iloop].private_rcvStamp = mgr.getCurrentTime();

          sprintf(thequery,"dome_logevent dome_logevent_detailedState:('%s', %lf , %lf , %d , %d , %d , %d , %d)"  , myData_logevent_detailedState[iloop].private_revCode.m_ptr , myData_logevent_detailedState[iloop].private_sndStamp , myData_logevent_detailedState[iloop].private_rcvStamp , myData_logevent_detailedState[iloop].private_seqNum , myData_logevent_detailedState[iloop].private_origin , myData_logevent_detailedState[iloop].private_host , myData_logevent_detailedState[iloop].detailedState , myData_logevent_detailedState[iloop].priority );
          cout << thequery << endl;

          err = RD_KAFKA_RESP_ERR_NO_ERROR;
	  len = strlen(thequery);
          if (rd_kafka_produce(rkt, partition,
                               RD_KAFKA_MSG_F_COPY,
                               thequery,
                               len,
                               /* No key, no opaque */
                               NULL, 0, NULL) == -1) {
              err = rd_kafka_last_error();
          }
          if (err) {
              fprintf(stderr,
                      "%% Failed to produce to topic %s "
                      "partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(err));

              /* Poll to handle delivery reports */
              rd_kafka_poll(rk, 0);
              continue;
          }
          if (!quiet)
              fprintf(stderr, "%% Sent %zd bytes to topic "
                      "%s partition %i\n",
                      len, rd_kafka_topic_name(rkt), partition);
          sendcnt++;
          /* Poll to handle delivery reports */
          rd_kafka_poll(rk, 0);

         }
        }
       }

       status = logevent_detailedState_SALReader->return_loan(myData_logevent_detailedState, logevent_detailedState_info);


       dome::logevent_errorCodeSeq myData_logevent_errorCode;
       SampleInfoSeq_var logevent_errorCode_info = new SampleInfoSeq;
       status = logevent_errorCode_SALReader->take(myData_logevent_errorCode, logevent_errorCode_info, 100, ANY_SAMPLE_STATE, ANY_VIEW_STATE, ANY_INSTANCE_STATE);
       mgr.checkStatus(status,"dome::logevent_errorCodeDataReader::take");
       numsamp = myData_logevent_errorCode.length();
       if (status == SAL__OK && numsamp > 0) {
        for (iloop=0;iloop<numsamp;iloop++) {
         if (myData_logevent_errorCode[iloop].private_origin != 0) {
          myData_logevent_errorCode[iloop].private_rcvStamp = mgr.getCurrentTime();

          sprintf(thequery,"dome_logevent_errorCode: ('%s', %lf , %lf , %d , %d , %d , %d , %d)"  , myData_logevent_errorCode[iloop].private_revCode.m_ptr , myData_logevent_errorCode[iloop].private_sndStamp , myData_logevent_errorCode[iloop].private_rcvStamp , myData_logevent_errorCode[iloop].private_seqNum , myData_logevent_errorCode[iloop].private_origin , myData_logevent_errorCode[iloop].private_host , myData_logevent_errorCode[iloop].errorCode , myData_logevent_errorCode[iloop].priority );
          cout << thequery << endl;

          err = RD_KAFKA_RESP_ERR_NO_ERROR;
	  len = strlen(thequery);
          if (rd_kafka_produce(rkt, partition,
                               RD_KAFKA_MSG_F_COPY,
                               thequery,
                               len,
                               /* No key, no opaque */
                               NULL, 0, NULL) == -1) {
              err = rd_kafka_last_error();
          }
          if (err) {
              fprintf(stderr,
                      "%% Failed to produce to topic %s "
                      "partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(err));

              /* Poll to handle delivery reports */
              rd_kafka_poll(rk, 0);
              continue;
          }
          if (!quiet)
              fprintf(stderr, "%% Sent %zd bytes to topic "
                      "%s partition %i\n",
                      len, rd_kafka_topic_name(rkt), partition);
          sendcnt++;
          /* Poll to handle delivery reports */
          rd_kafka_poll(rk, 0);
         }
        }
       }
       status = logevent_errorCode_SALReader->return_loan(myData_logevent_errorCode, logevent_errorCode_info);


       dome::logevent_settingVersionsSeq myData_logevent_settingVersions;
       SampleInfoSeq_var logevent_settingVersions_info = new SampleInfoSeq;
       status = logevent_settingVersions_SALReader->take(myData_logevent_settingVersions, logevent_settingVersions_info, 100, ANY_SAMPLE_STATE, ANY_VIEW_STATE, ANY_INSTANCE_STATE);
       mgr.checkStatus(status,"dome::logevent_settingVersionsDataReader::take");
       numsamp = myData_logevent_settingVersions.length();
       if (status == SAL__OK && numsamp > 0) {
        for (iloop=0;iloop<numsamp;iloop++) {
         if (myData_logevent_settingVersions[iloop].private_origin != 0) {
          myData_logevent_settingVersions[iloop].private_rcvStamp = mgr.getCurrentTime();

          sprintf(thequery,"dome_logevent_settingVersions: ('%s', %lf , %lf , %d , %d , %d , '%s' , %d)"  , myData_logevent_settingVersions[iloop].private_revCode.m_ptr , myData_logevent_settingVersions[iloop].private_sndStamp , myData_logevent_settingVersions[iloop].private_rcvStamp , myData_logevent_settingVersions[iloop].private_seqNum , myData_logevent_settingVersions[iloop].private_origin , myData_logevent_settingVersions[iloop].private_host , myData_logevent_settingVersions[iloop].recommendedSettingVersion.m_ptr , myData_logevent_settingVersions[iloop].priority );
          cout << thequery << endl;

          err = RD_KAFKA_RESP_ERR_NO_ERROR;
	  len = strlen(thequery);
          if (rd_kafka_produce(rkt, partition,
                               RD_KAFKA_MSG_F_COPY,
                               thequery,
                               len,
                               /* No key, no opaque */
                               NULL, 0, NULL) == -1) {
              err = rd_kafka_last_error();
          }
          if (err) {
              fprintf(stderr,
                      "%% Failed to produce to topic %s "
                      "partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(err));

              /* Poll to handle delivery reports */
              rd_kafka_poll(rk, 0);
              continue;
          }
          if (!quiet)
              fprintf(stderr, "%% Sent %zd bytes to topic "
                      "%s partition %i\n",
                      len, rd_kafka_topic_name(rkt), partition);
          sendcnt++;
          /* Poll to handle delivery reports */
          rd_kafka_poll(rk, 0);
         }
        }
       }
       status = logevent_settingVersions_SALReader->return_loan(myData_logevent_settingVersions, logevent_settingVersions_info);


       dome::logevent_stateChangedSeq myData_logevent_stateChanged;
       SampleInfoSeq_var logevent_stateChanged_info = new SampleInfoSeq;
       status = logevent_stateChanged_SALReader->take(myData_logevent_stateChanged, logevent_stateChanged_info, 100, ANY_SAMPLE_STATE, ANY_VIEW_STATE, ANY_INSTANCE_STATE);
       mgr.checkStatus(status,"dome::logevent_stateChangedDataReader::take");
       numsamp = myData_logevent_stateChanged.length();
       if (status == SAL__OK && numsamp > 0) {
        for (iloop=0;iloop<numsamp;iloop++) {
         if (myData_logevent_stateChanged[iloop].private_origin != 0) {
          myData_logevent_stateChanged[iloop].private_rcvStamp = mgr.getCurrentTime();

          sprintf(thequery,"dome_logevent_stateChanged: ('%s', %lf , %lf , %d , %d , %d , %d , %d)"  , myData_logevent_stateChanged[iloop].private_revCode.m_ptr , myData_logevent_stateChanged[iloop].private_sndStamp , myData_logevent_stateChanged[iloop].private_rcvStamp , myData_logevent_stateChanged[iloop].private_seqNum , myData_logevent_stateChanged[iloop].private_origin , myData_logevent_stateChanged[iloop].private_host , myData_logevent_stateChanged[iloop].newState , myData_logevent_stateChanged[iloop].priority );
          cout << thequery << endl;


          err = RD_KAFKA_RESP_ERR_NO_ERROR;
	  len = strlen(thequery);
          if (rd_kafka_produce(rkt, partition,
                               RD_KAFKA_MSG_F_COPY,
                               thequery,
                               len,
                               /* No key, no opaque */
                               NULL, 0, NULL) == -1) {
              err = rd_kafka_last_error();
          }
          if (err) {
              fprintf(stderr,
                      "%% Failed to produce to topic %s "
                      "partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(err));

              /* Poll to handle delivery reports */
              rd_kafka_poll(rk, 0);
              continue;
          }
          if (!quiet)
              fprintf(stderr, "%% Sent %zd bytes to topic "
                      "%s partition %i\n",
                      len, rd_kafka_topic_name(rkt), partition);
          sendcnt++;
          /* Poll to handle delivery reports */
          rd_kafka_poll(rk, 0);

         }
        }
       }
       status = logevent_stateChanged_SALReader->return_loan(myData_logevent_stateChanged, logevent_stateChanged_info);


       dome::logevent_summaryStateSeq myData_logevent_summaryState;
       SampleInfoSeq_var logevent_summaryState_info = new SampleInfoSeq;
       status = logevent_summaryState_SALReader->take(myData_logevent_summaryState, logevent_summaryState_info, 100, ANY_SAMPLE_STATE, ANY_VIEW_STATE, ANY_INSTANCE_STATE);
       mgr.checkStatus(status,"dome::logevent_summaryStateDataReader::take");
       numsamp = myData_logevent_summaryState.length();
       if (status == SAL__OK && numsamp > 0) {
        for (iloop=0;iloop<numsamp;iloop++) {
         if (myData_logevent_summaryState[iloop].private_origin != 0) {
          myData_logevent_summaryState[iloop].private_rcvStamp = mgr.getCurrentTime();

          sprintf(thequery,"dome_logevent_summaryState: ('%s', %lf , %lf , %d , %d , %d , %d , %d)"  , myData_logevent_summaryState[iloop].private_revCode.m_ptr , myData_logevent_summaryState[iloop].private_sndStamp , myData_logevent_summaryState[iloop].private_rcvStamp , myData_logevent_summaryState[iloop].private_seqNum , myData_logevent_summaryState[iloop].private_origin , myData_logevent_summaryState[iloop].private_host , myData_logevent_summaryState[iloop].summaryState , myData_logevent_summaryState[iloop].priority );
          cout << thequery << endl;


          err = RD_KAFKA_RESP_ERR_NO_ERROR;
	  len = strlen(thequery);
          if (rd_kafka_produce(rkt, partition,
                               RD_KAFKA_MSG_F_COPY,
                               thequery,
                               len,
                               /* No key, no opaque */
                               NULL, 0, NULL) == -1) {
              err = rd_kafka_last_error();
          }
          if (err) {
              fprintf(stderr,
                      "%% Failed to produce to topic %s "
                      "partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(err));

              /* Poll to handle delivery reports */
              rd_kafka_poll(rk, 0);
              continue;
          }
          if (!quiet)
              fprintf(stderr, "%% Sent %zd bytes to topic "
                      "%s partition %i\n",
                      len, rd_kafka_topic_name(rkt), partition);
          sendcnt++;
          /* Poll to handle delivery reports */
          rd_kafka_poll(rk, 0);

         }
        }
       }
       status = logevent_summaryState_SALReader->return_loan(myData_logevent_summaryState, logevent_summaryState_info);


     os_nanoSleep(delay_10us);
  }

  /* This bit replaces MySQL shutdown wtih Kafka shutdown. */
  /* Poll to handle delivery reports */
  rd_kafka_poll(rk, 0);
  
  /* Wait for messages to be delivered */
  while (run && rd_kafka_outq_len(rk) > 0)
      rd_kafka_poll(rk, 100);
  
  /* Destroy topic */
  rd_kafka_topic_destroy(rkt);
  
  /* Destroy the handle */
  rd_kafka_destroy(rk);
  
  mgr.salShutdown();


  return 0;
}

int main (int argc, char **argv)
{
  return test_dome_event_kafkawriter();
}

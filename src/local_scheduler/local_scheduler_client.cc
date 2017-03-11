#include "local_scheduler_client.h"

#include "common/io.h"
#include "common/task.h"
#include <stdlib.h>

LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket,
    ActorID actor_id) {
  LocalSchedulerConnection *result =
      (LocalSchedulerConnection *) malloc(sizeof(LocalSchedulerConnection));
  result->conn = connect_ipc_sock_retry(local_scheduler_socket, -1, -1);
  register_worker_info info;
  memset(&info, 0, sizeof(info));
  /* Register the process ID with the local scheduler. */
  info.worker_pid = getpid();
  info.actor_id = actor_id;
  int success = write_message(result->conn, REGISTER_WORKER_INFO, sizeof(info),
                              (uint8_t *) &info);
  CHECKM(success == 0, "Unable to register worker with local scheduler");
  return result;
}

void LocalSchedulerConnection_free(LocalSchedulerConnection *conn) {
  close(conn->conn);
  free(conn);
}

void local_scheduler_log_event(LocalSchedulerConnection *conn,
                               uint8_t *key,
                               int64_t key_length,
                               uint8_t *value,
                               int64_t value_length) {
  int64_t message_length =
      sizeof(key_length) + sizeof(value_length) + key_length + value_length;
  uint8_t *message = (uint8_t *) malloc(message_length);
  int64_t offset = 0;
  memcpy(&message[offset], &key_length, sizeof(key_length));
  offset += sizeof(key_length);
  memcpy(&message[offset], &value_length, sizeof(value_length));
  offset += sizeof(value_length);
  memcpy(&message[offset], key, key_length);
  offset += key_length;
  memcpy(&message[offset], value, value_length);
  offset += value_length;
  CHECK(offset == message_length);
  write_message(conn->conn, EVENT_LOG_MESSAGE, message_length, message);
  free(message);
}

void local_scheduler_submit(LocalSchedulerConnection *conn,
                            TaskSpec *task,
                            int64_t task_size) {
  write_message(conn->conn, SUBMIT_TASK, task_size, (uint8_t *) task);
}

TaskSpec *local_scheduler_get_task(LocalSchedulerConnection *conn,
                                   int64_t *task_size) {
  write_message(conn->conn, GET_TASK, 0, NULL);
  int64_t type;
  uint8_t *message;
  /* Receive a task from the local scheduler. This will block until the local
   * scheduler gives this client a task. */
  read_message(conn->conn, &type, task_size, &message);
  CHECK(type == EXECUTE_TASK);
  TaskSpec *task = (TaskSpec *) message;
  return task;
}

void local_scheduler_task_done(LocalSchedulerConnection *conn) {
  write_message(conn->conn, TASK_DONE, 0, NULL);
}

void local_scheduler_reconstruct_object(LocalSchedulerConnection *conn,
                                        ObjectID object_id) {
  write_message(conn->conn, RECONSTRUCT_OBJECT, sizeof(object_id),
                (uint8_t *) &object_id);
  /* TODO(swang): Propagate the error. */
}

void local_scheduler_log_message(LocalSchedulerConnection *conn) {
  write_message(conn->conn, LOG_MESSAGE, 0, NULL);
}

void local_scheduler_notify_unblocked(LocalSchedulerConnection *conn) {
  write_message(conn->conn, NOTIFY_UNBLOCKED, 0, NULL);
}
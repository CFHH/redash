import signal
import time
import redis

from rq import get_current_job
from rq.job import JobStatus
from rq.timeouts import JobTimeoutException
from rq.exceptions import NoSuchJobError

from redash import models, redis_connection, settings
from redash.query_runner import InterruptException
from redash.tasks.worker import Queue, Job
from redash.tasks.alerts import check_alerts_for_query
from redash.tasks.failure_report import track_failure
from redash.utils import gen_query_hash, json_dumps, utcnow
from redash.worker import get_job_logger

TIMEOUT_MESSAGE = "Query exceeded Redash query execution time limit."
def get_logger():
    return get_job_logger(__name__)


def _job_lock_id(query_hash, data_source_id):
    return "query_hash_job:%s:%s" % (data_source_id, query_hash)


def _unlock(query_hash, data_source_id):
    redis_connection.delete(_job_lock_id(query_hash, data_source_id))


def enqueue_query(
    query, data_source, user_id, is_api_key=False, scheduled_query=None, metadata={}
):
    query_id = metadata.get("Query ID", "unknown")
    query_hash = gen_query_hash(query)
    get_logger().info("[query_id=%s] [query_hash=%s] Inserting job", query_id, query_hash)
    try_count = 0
    job = None

    while try_count < 5:
        try_count += 1

        pipe = redis_connection.pipeline()
        try:
            pipe.watch(_job_lock_id(query_hash, data_source.id))
            job_id = pipe.get(_job_lock_id(query_hash, data_source.id))
            if job_id:
                job_status = "UNKNOWN"
                job_complete = False
                job_cancelled = "False"

                try:
                    job = Job.fetch(job_id)
                    job_exists = True
                    job_status = job.get_status()
                    job_complete = job_status in [JobStatus.FINISHED, JobStatus.FAILED]
                    if job.is_cancelled:
                        job_cancelled = "True"
                except NoSuchJobError:
                    job_exists = False
                    job_status = "EXPIRED"

                get_logger().info("[query_id=%s] [query_hash=%s] Found existing job [job.id=%s] [job_status=%s] [job_cancelled=%s]", query_id, query_hash, job_id, job_status, job_cancelled)

                if job_complete or (not job_exists):
                    #get_logger().info("[query_id=%s] [query_hash=%s] [job.id=%s], removing redis lock", query_id, query_hash, job_id)
                    redis_connection.delete(_job_lock_id(query_hash, data_source.id))
                    job = None

            if not job:
                pipe.multi()

                if scheduled_query:
                    queue_name = data_source.scheduled_queue_name  #默认都是scheduled_queries
                    scheduled_query_id = scheduled_query.id
                else:
                    queue_name = data_source.queue_name #默认都是queries
                    scheduled_query_id = None

                time_limit = settings.dynamic_settings.query_time_limit(
                    scheduled_query, user_id, data_source.org_id
                )
                metadata["Queue"] = queue_name
                metadata["Enqueue Time"] = time.time()

                queue = Queue(queue_name)
                enqueue_kwargs = {
                    "user_id": user_id,
                    "scheduled_query_id": scheduled_query_id,
                    "is_api_key": is_api_key,
                    "job_timeout": time_limit,
                    "meta": {
                        "data_source_id": data_source.id,
                        "org_id": data_source.org_id,
                        "scheduled": scheduled_query_id is not None,
                        "query_id": query_id,
                        "user_id": user_id,
                    },
                }

                if not scheduled_query:
                    enqueue_kwargs["result_ttl"] = settings.JOB_EXPIRY_TIME

                job = queue.enqueue(
                    execute_query, query, data_source.id, metadata, **enqueue_kwargs
                )

                get_logger().info("[query_id=%s] [query_hash=%s] Created new job [job.id=%s]", query_id, query_hash, job.id)
                pipe.set(
                    _job_lock_id(query_hash, data_source.id),
                    job.id,
                    settings.JOB_EXPIRY_TIME,
                )
                pipe.execute()
            break

        except redis.WatchError:
            get_logger().error("[query_id=%s] [query_hash=%s] redis.WatchError, try_count = %d", query_id, query_hash, try_count)
            continue

    if not job:
        get_logger().error("[Manager] [query_id=%s] [query_hash=%s] Failed adding job for query.", query_id, query_hash)

    return job


def signal_handler(*args):
    raise InterruptException


class QueryExecutionError(Exception):
    pass


def _resolve_user(user_id, is_api_key, query_id):
    if user_id is not None:
        if is_api_key:
            api_key = user_id
            if query_id is not None:
                q = models.Query.get_by_id(query_id)
            else:
                q = models.Query.by_api_key(api_key)

            return models.ApiUser(api_key, q.org, q.groups)
        else:
            return models.User.get_by_id(user_id)
    else:
        return None


class QueryExecutor(object):
    def __init__(
        self, query, data_source_id, user_id, is_api_key, metadata, scheduled_query
    ):
        self.job = get_current_job()
        self.query = query
        self.query_hash = gen_query_hash(self.query)
        self.data_source_id = data_source_id
        self.metadata = metadata
        self.data_source = self._load_data_source()
        self.user = _resolve_user(user_id, is_api_key, metadata.get("Query ID"))

        # Close DB connection to prevent holding a connection for a long time while the query is executing.
        models.db.session.close()
        self.scheduled_query = scheduled_query
        # Load existing tracker or create a new one if the job was created before code update:
        if scheduled_query:
            models.scheduled_queries_executions.update(scheduled_query.id)

    def run(self):
        started_at = time.time()
        signal.signal(signal.SIGINT, signal_handler)

        enqueue_time = self.metadata.get("Enqueue Time", 0.0)
        waiting_time = started_at - enqueue_time
        message = "waiting_time=%f" % waiting_time

        self._log_progress("EXECUTING_QUERY", message)

        query_runner = self.data_source.query_runner
        annotated_query = self._annotate_query(query_runner)

        try:
            data, error = query_runner.run_query(annotated_query, self.user)
        except Exception as e:
            if isinstance(e, JobTimeoutException):
                error = TIMEOUT_MESSAGE
            else:
                error = str(e)

            data = None
            #get_logger().warning("Unexpected error while running query:", exc_info=1)
            run_time = time.time() - started_at
            message = "run_time=%f, error=[%s]" % (run_time, error)
            self._log_progress("UNEXPECTED_ERROR", message)

        run_time = time.time() - started_at
        message = "run_time=%f, error=[%s], data_length=%s" % (run_time, error, data and len(data))
        self._log_progress("AFTER_QUERY", message)

        _unlock(self.query_hash, self.data_source.id)

        if error is not None and data is None:
            result = QueryExecutionError(error)
            if self.scheduled_query is not None:
                self.scheduled_query = models.db.session.merge(
                    self.scheduled_query, load=False
                )
                track_failure(self.scheduled_query, error)
            raise result
        else:
            if self.scheduled_query and self.scheduled_query.schedule_failures > 0:
                self.scheduled_query = models.db.session.merge(
                    self.scheduled_query, load=False
                )
                self.scheduled_query.schedule_failures = 0
                self.scheduled_query.skip_updated_at = True
                models.db.session.add(self.scheduled_query)

            query_result = models.QueryResult.store_result(
                self.data_source.org_id,
                self.data_source,
                self.query_hash,
                self.query,
                data,
                run_time,
                utcnow(),
            )

            updated_query_ids = models.Query.update_latest_result(query_result)

            models.db.session.commit()  # make sure that alert sees the latest query result
            self._log_progress("CHECKING_ALERTS")
            for query_id in updated_query_ids:
                check_alerts_for_query.delay(query_id)
            self._log_progress("FINISHED")

            result = query_result.id
            models.db.session.commit()
            return result

    def _annotate_query(self, query_runner):
        self.metadata["Job ID"] = self.job.id
        self.metadata["Query Hash"] = self.query_hash
        self.metadata["Scheduled"] = self.scheduled_query is not None

        return query_runner.annotate_query(self.query, self.metadata)

    def _log_progress(self, state, message=''):
        get_logger().info(
            "job=execute_query [state=%s] [query_id=%s] [query_hash=%s] [ds_id=%d] [ds_type=%s] [queue=%s], %s",
            state,
            self.metadata.get("Query ID", "unknown"),
            self.query_hash,
            self.data_source_id,
            self.data_source and self.data_source.type,
            self.metadata.get("Queue", "unknown"),
            message,
        )

    def _load_data_source(self):
        self.data_source = None
        self._log_progress("LOADING_DS")
        return models.DataSource.query.get(self.data_source_id)


# user_id is added last as a keyword argument for backward compatability -- to support executing previously submitted
# jobs before the upgrade to this version.
def execute_query(
    query,
    data_source_id,
    metadata,
    user_id=None,
    scheduled_query_id=None,
    is_api_key=False,
):
    if scheduled_query_id is not None:
        scheduled_query = models.Query.query.get(scheduled_query_id)
    else:
        scheduled_query = None

    try:
        return QueryExecutor(
            query, data_source_id, user_id, is_api_key, metadata, scheduled_query
        ).run()
    except QueryExecutionError as e:
        models.db.session.rollback()
        return e

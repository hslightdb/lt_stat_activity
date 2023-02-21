

#include "postgres.h"

#include <unistd.h>
#include <sys/time.h>
#include <stdio.h>
#include "access/twophase.h"
#include "executor/executor.h"
#include "parser/analyze.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "tcop/utility.h"
#include "access/parallel.h"
PG_MODULE_MAGIC;

/*
 * Global shared state
 */
typedef struct ltsaSharedState
{
	LWLock	    *lock;
	uint64		queryids[0];
} ltsaSharedState;

/*---- Local variables ----*/

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
static int	exec_nested_level = 0;

/* Current nesting depth of planner calls */
static int	plan_nested_level = 0;


/* saved hook address in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* Links to shared memory state */
static ltsaSharedState *ltsa = NULL;


/*---- GUC variables ----*/

typedef enum
{
	LTSA_TRACK_NONE,			/* track no statements */
	LTSA_TRACK_TOP,				/* only top level statements */
}LTSATrackLevel;

static const struct config_enum_entry track_options[] =
{
	{"none", LTSA_TRACK_NONE, false},
	{"top",  LTSA_TRACK_TOP, false},
	{NULL, 0, false}
};

static int	ltsa_track;			/* tracking level */

#define ltsa_enabled(level) \
	((!IsParallelWorker()) && (ltsa_track == LTSA_TRACK_TOP && (level) == 0))

#define ltsa_is_enable() (ltsa_track != LTSA_TRACK_NONE)

/*--- Functions --- */

void	_PG_init(void);
void	_PG_fini(void);

static Size ltsa_memsize(void);

static void ltsa_shmem_startup(void);

static void ltsa_post_parse_analyze(ParseState *pstate, Query *query);
static void ltsa_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void ltsa_ExecutorRun(QueryDesc *queryDesc,
							 ScanDirection direction,
							 uint64 count, bool execute_once);
static void ltsa_ExecutorFinish(QueryDesc *queryDesc);

static void ltsa_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								ProcessUtilityContext context, ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest, QueryCompletion *qc);
static int get_max_procs_count(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}

	DefineCustomEnumVariable("lt_stat_activity.track",
							 "Selects which statements are tracked by lt_stat_activity.",
							 NULL,
							 &ltsa_track,
							 LTSA_TRACK_TOP,
							 track_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	RequestAddinShmemSpace(ltsa_memsize());
	RequestNamedLWLockTranche("lt_stat_activity", 1);

	/* Install hook */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ltsa_shmem_startup;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = ltsa_post_parse_analyze;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = ltsa_ExecutorStart;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = ltsa_ExecutorRun;

	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = ltsa_ExecutorFinish;

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = ltsa_ProcessUtility;
}

void
_PG_fini(void)
{
	/* uninstall hook */
	shmem_startup_hook = prev_shmem_startup_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ProcessUtility_hook = ProcessUtility_hook;
}

static Size 
ltsa_memsize(void)
{
	Size	size;
	size = MAXALIGN(sizeof(ltsaSharedState));
	size = add_size(size, MAXALIGN(sizeof(uint64) * get_max_procs_count()));
	return size;
}

static void
ltsa_shmem_startup(void)
{
	bool		found;
	
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	ltsa = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ltsa = ShmemInitStruct("lt_stat_activity", 
					ltsa_memsize(),
					&found);

	if (!found)
	{
		/* First time through ... */
		LWLockPadded *locks = GetNamedLWLockTranche("lt_stat_activity");
		ltsa->lock = &(locks[0]).lock;
		MemSet(ltsa->queryids, 0, MAXALIGN(sizeof(uint64) * get_max_procs_count()));
	}

	LWLockRelease(AddinShmemInitLock);
}

static void ltsa_set_queryid(uint64 queryid)
{
	int i = 0;
	if (MyProc == NULL)
	{
		return;
	}

	i = MyProc - ProcGlobal->allProcs;
	if (ltsa->queryids[i] == queryid) 
	{
		return;
	}
	/** 
	 *  update is no conflict with each other, but conflict with query.
	 *  because updates are more frequent, use shared lock when updating and exclusive when querying.  
	 */
	LWLockAcquire(ltsa->lock, LW_SHARED);
	ltsa->queryids[i] = queryid;
	LWLockRelease(ltsa->lock);
}

static void 
ltsa_post_parse_analyze(ParseState *pstate, Query *query)
{
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query);
	
	if (!ltsa_enabled(exec_nested_level + plan_nested_level))
		return;
	ltsa_set_queryid(query->queryId);
}


static int
get_max_procs_count(void)
{
	int count = 0;
	/* MyProcs, including autovacuum workers and launcher */
	count += MaxBackends;
	/* AuxiliaryProcs */
	count += NUM_AUXILIARY_PROCS;
	/* Prepared xacts */
	count += max_prepared_xacts;

	return count;
}

static void ltsa_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (ltsa_enabled(exec_nested_level + plan_nested_level))
		ltsa_set_queryid(queryDesc->plannedstmt->queryId);
	
	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorStart)
			prev_ExecutorStart(queryDesc, eflags);
		else
			standard_ExecutorStart(queryDesc, eflags);
	}
	PG_FINALLY();
	{
		exec_nested_level--;
	}
	PG_END_TRY();	
}
/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
ltsa_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
				 bool execute_once)
{
	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		exec_nested_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
ltsa_ExecutorFinish(QueryDesc *queryDesc)
{
	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		exec_nested_level--;
	}
	PG_END_TRY();
}

static void ltsa_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								ProcessUtilityContext context, ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest, QueryCompletion *qc)
								{
	if (ltsa_enabled(exec_nested_level))
	{
		ltsa_set_queryid(UINT64CONST(0));
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString,
								context, params, queryEnv,
								dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, qc);
	}
	PG_FINALLY();
	{
		exec_nested_level--;
	}
	PG_END_TRY();
}

typedef struct
{
	uint32			pid;
	int 			idx;
	uint64			queryid;
} QuieyIdItem;

PG_FUNCTION_INFO_V1(lt_stat_activity_get_query_id);
Datum
lt_stat_activity_get_query_id(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	QuieyIdItem	   *queryIdItems;
	int procCount;
	int i = 0;
	int active_proc_count = 0;

	
	if (SRF_IS_FIRSTCALL())
	{
		
		MemoryContext		oldcontext;
		TupleDesc			tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		tupdesc = CreateTemplateTupleDesc(2);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "query_id",
						   INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		if (ltsa_is_enable()) 
		{
			LWLockAcquire(ProcArrayLock, LW_SHARED);
			procCount = ProcGlobal->allProcCount;
			queryIdItems = (QuieyIdItem *) palloc0(sizeof(QuieyIdItem) * procCount);
			for (i = 0; i < procCount; i++)
			{
				PGPROC *proc = &ProcGlobal->allProcs[i];
				if (proc != NULL && proc->pid != 0)
				{
					queryIdItems[active_proc_count].pid = proc->pid;
					queryIdItems[active_proc_count].idx = i;
					active_proc_count++;
				}
			}
			LWLockRelease(ProcArrayLock);
		
			LWLockAcquire(ltsa->lock, LW_EXCLUSIVE);
			for (i = 0; i < active_proc_count; i++)
			{
				queryIdItems[i].queryid = ltsa->queryids[queryIdItems[i].idx];
			}
			LWLockRelease(ltsa->lock);
		
			funcctx->user_fctx = queryIdItems;
			funcctx->max_calls = active_proc_count;
		}
		else 
		{
			funcctx->user_fctx = NULL;
			funcctx->max_calls = 0;
		}
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	queryIdItems = (QuieyIdItem *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls && queryIdItems != NULL)
	{
		HeapTuple	tuple;
		QuieyIdItem * queryIdItem;
		Datum		values[2];
		bool		nulls[2];

		queryIdItem = &queryIdItems[funcctx->call_cntr];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(queryIdItem->pid);
		nulls[0] = false;
		if (queryIdItem->queryid > 0) 
		{
			values[1] = Int64GetDatumFast(queryIdItem->queryid);
			nulls[1] = false;
		}
		else 
		{
			nulls[1] = true;
		}
		
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

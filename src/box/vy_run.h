#ifndef INCLUDES_TARANTOOL_BOX_VY_RUN_H
#define INCLUDES_TARANTOOL_BOX_VY_RUN_H
/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <stdint.h>
#include <stdbool.h>

#include "small/rlist.h"
#include "small/mempool.h"
#include "salad/bloom.h"

#include "index.h" /* enum iterator_type */
#include "vy_stmt.h" /* for comparators */
#include "vy_stmt_iterator.h" /* struct vy_stmt_iterator */
#include "coeio.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/** Part of vinyl environment for run read/write */
struct vy_run_env {
	/** Mempool for struct vy_page_read_task */
	struct mempool read_task_pool;
	/** Key for thread-local ZSTD context */
	pthread_key_t zdctx_key;
};

/**
 * Run metadata. A run is a written to a file as a single
 * chunk.
 */
struct vy_run_info {
	/** Run page count. */
	uint32_t  count;
	/** Number of keys. */
	uint32_t  keys;
	/* Min and max lsn over all statements in the run. */
	int64_t  min_lsn;
	int64_t  max_lsn;
	/** Size of run on disk. */
	uint64_t size;
	/** Bloom filter of all tuples in run */
	bool has_bloom;
	struct bloom bloom;
	/** Pages meta. */
	struct vy_page_info *page_infos;
};

struct vy_page_info {
	/* count of statements in the page */
	uint32_t count;
	/* offset of page data in run */
	uint64_t offset;
	/* size of page data in file */
	uint32_t size;
	/* size of page data in memory, i.e. unpacked */
	uint32_t unpacked_size;
	/* Offset of the min key in the parent run->pages_min. */
	uint32_t min_key_offset;
	/* minimal lsn of all records in page */
	int64_t min_lsn;
	/* maximal lsn of all records in page */
	int64_t max_lsn;
	/* minimal key */
	char *min_key;
	/* row index offset in page */
	uint32_t page_index_offset;
};

struct vy_run {
	struct vy_run_info info;
	/** Run data file. */
	int fd;
	/**
	 * Reference counter. The run file is closed and the run
	 * in-memory structure is freed only when it reaches 0.
	 * Needed to prevent coeio thread from using a closed
	 * (worse, reopened) file descriptor.
	 */
	int refs;
	/** Link in range->runs list. */
	struct rlist in_range;
	/** Unique ID of this run. */
	int64_t id;
};

/**
 * coio task for vinyl page read
 */
struct vy_page_read_task {
	/** parent */
	struct coio_task base;
	/** vinyl page metadata */
	struct vy_page_info page_info;
	/** vy_run with fd - ref. counted */
	struct vy_run *run;
	/** vy_run_env - contains environment with task mempool */
	struct vy_run_env *run_env;
	/** [out] resulting vinyl page */
	struct vy_page *page;
	/** [out] result code */
	int rc;
};


/** Argument passed to vy_join_cb(). */
struct vy_join_arg {
	/** Vinyl run environment. */
	struct vy_run_env *env;
	/* path to vinyl_dir */
	char *path;
	/** Recovery context to relay. */
	struct vy_recovery *recovery;
	/** Stream to relay statements to. */
	struct xstream *stream;
	/** ID of the space currently being relayed. */
	uint32_t space_id;
	/** Ordinal number of the index. */
	uint32_t index_id;
	/** Path to the index directory. */
	char *index_path;
	/**
	 * LSN to assign to the next statement.
	 *
	 * We can't use original statements' LSNs, because we
	 * send statements not in the chronological order while
	 * the receiving end expects LSNs to grow monotonically
	 * due to the design of the lsregion allocator, which is
	 * used for storing statements in memory.
	 */
	int64_t lsn;
};

/**
 * Page
 */
struct vy_page {
	/** Page position in the run file (used by run_iterator->page_cache */
	uint32_t page_no;
	/** The number of statements */
	uint32_t count;
	/** Page data size */
	uint32_t unpacked_size;
	/** Array with row offsets in page data */
	uint32_t *page_index;
	/** Page data */
	char *data;
};

/* {{{ Run iterator */

/** Position of a particular stmt in vy_run. */
struct vy_run_iterator_pos {
	uint32_t page_no;
	uint32_t pos_in_page;
};

/**
 * Return statements from vy_run based on initial search key,
 * iteration order and view lsn.
 *
 * All statements with lsn > vlsn are skipped.
 * The API allows to traverse over resulting statements within two
 * dimensions - key and lsn. next_key() switches to the youngest
 * statement of the next key, according to the iteration order,
 * and next_lsn() switches to an older statement for the same
 * key.
 */
struct vy_run_iterator {
	/** Parent class, must be the first member */
	struct vy_stmt_iterator base;
	/** Usage statistics */
	struct vy_iterator_stat *stat;

	/* environment for memory allocation and disk access */
	struct vy_run_env *run_env;
	/* comparison arg */
	struct index_def *index_def;
	/* original index_def */
	struct index_def *user_index_def;
	/* should the iterator use coio task for reading or not */
	bool coio_read;
	/**
	 * Format ot allocate REPLACE and DELETE tuples read from
	 * pages.
	 */
	struct tuple_format *format;
	/** Same as format, but for UPSERT tuples. */
	struct tuple_format *upsert_format;
	/* run */
	struct vy_run *run;

	/* Search options */
	/**
	 * Iterator type, that specifies direction, start position and stop
	 * criteria if the key is not specified, GT and EQ are changed to
	 * GE, LT to LE for beauty.
	 */
	enum iterator_type iterator_type;
	/** Key to search. */
	const struct tuple *key;
	/* LSN visibility, iterator shows values with lsn <= vlsn */
	const int64_t *vlsn;

	/* State of the iterator */
	/** Position of the current record */
	struct vy_run_iterator_pos curr_pos;
	/**
	 * Last stmt returned by vy_run_iterator_get.
	 * The iterator holds this stmt until the next call to
	 * vy_run_iterator_get, when it's dereferenced.
	 */
	struct tuple *curr_stmt;
	/** Position of record that spawned curr_stmt */
	struct vy_run_iterator_pos curr_stmt_pos;
	/** LRU cache of two active pages (two pages is enough). */
	struct vy_page *curr_page;
	struct vy_page *prev_page;
	/** Is false until first .._get or .._next_.. method is called */
	bool search_started;
	/** Search is finished, you will not get more values from iterator */
	bool search_ended;
};
/* }}} Run iterator */

struct vy_log_record;

/** Relay callback, passed to vy_recovery_iterate(). */
int
vy_join_cb(const struct vy_log_record *record, void *cb_arg);

/**
 * Initialize vinyl run environment
 */
void
vy_run_env_create(struct vy_run_env *env);

/**
 * Destroy vinyl run environment
 */
void
vy_run_env_destroy(struct vy_run_env *env);

/**
 * Initialize page info struct
 *
 * @retval 0 for Success
 * @retval -1 for error
 */
int
vy_page_info_create(struct vy_page_info *page_info, uint64_t offset,
		    const struct index_def *index_def, struct tuple *min_key);

struct vy_run *
vy_run_new(int64_t id);

void
vy_run_delete(struct vy_run *run);

static inline uint64_t
vy_run_size(struct vy_run *run)
{
	return run->info.size;
}

static inline bool
vy_run_is_empty(struct vy_run *run)
{
	return run->info.count == 0;
}

static inline struct vy_page_info *
vy_run_page_info(struct vy_run *run, uint32_t pos)
{
	assert(pos < run->info.count);
	return &run->info.page_infos[pos];
}

/** Increment a run's reference counter. */
static inline void
vy_run_ref(struct vy_run *run)
{
	assert(run->refs > 0);
	run->refs++;
}

/*
 * Decrement a run's reference counter.
 * Return true if the run was deleted.
 */
static inline bool
vy_run_unref(struct vy_run *run)
{
	assert(run->refs > 0);
	if (--run->refs == 0) {
		vy_run_delete(run);
		return true;
	}
	return false;
}

int
vy_run_recover(struct vy_run *run, const char *dir);

int
vy_join_cb(const struct vy_log_record *record, void *cb_arg);

/* {{{ export part for run writing */
/* TODO: delete from .h and make static in .c after moving writing part to .c */

static const uint64_t vy_run_info_key_map = (1 << VY_RUN_INFO_MIN_LSN) |
					    (1 << VY_RUN_INFO_MAX_LSN) |
					    (1 << VY_RUN_INFO_PAGE_COUNT);

static const uint64_t vy_page_info_key_map = (1 << VY_PAGE_INFO_OFFSET) |
					     (1 << VY_PAGE_INFO_SIZE) |
					     (1 << VY_PAGE_INFO_UNPACKED_SIZE) |
					     (1 << VY_PAGE_INFO_ROW_COUNT) |
					     (1 << VY_PAGE_INFO_MIN_KEY) |
					     (1 << VY_PAGE_INFO_PAGE_INDEX_OFFSET);

enum { VY_BLOOM_VERSION = 0 };

enum vy_file_type {
	VY_FILE_INDEX,
	VY_FILE_RUN,
	vy_file_MAX,
};

/** xlog meta type for .run files */
#define XLOG_META_TYPE_RUN "RUN"

/** xlog meta type for .index files */
#define XLOG_META_TYPE_INDEX "INDEX"

int
vy_index_snprint_path(char *buf, int size, const char *dir,
		      uint32_t space_id, uint32_t iid);

int
vy_run_snprint_name(char *buf, int size, int64_t run_id, enum vy_file_type type);

int
vy_run_snprint_path(char *buf, int size, const char *dir,
		    int64_t run_id, enum vy_file_type type);

/** Return the path to the run encoded by a metadata log record. */
int
vy_run_record_snprint_path(char *buf, int size, const char *vinyl_dir,
			   const struct vy_log_record *record,
			   enum vy_file_type type);

/* }}} export part for run writing */

/* {{{ Run iterator */

void
vy_run_iterator_open(struct vy_run_iterator *itr, bool coio_read,
		     struct vy_iterator_stat *stat, struct vy_run_env *run_env,
		     struct index_def *index_def, struct index_def *user_index_def,
		     struct vy_run *run, enum iterator_type iterator_type,
		     const struct tuple *key, const int64_t *vlsn,
		     struct tuple_format *format,
		     struct tuple_format *upsert_format);

/* }}} Run iterator */

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* INCLUDES_TARANTOOL_BOX_VY_RUN_H */

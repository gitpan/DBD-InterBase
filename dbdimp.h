/*
   $Id: dbdimp.h,v 1.9 2001/02/12 18:24:31 edpratomo Exp $

   Copyright (c) 1999-2001  Edwin Pratomo

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.

*/

#include <DBIXS.h>              /* installed by the DBI module  */

static const int DBI_SQL_CHAR       = SQL_CHAR;
static const int DBI_SQL_NUMERIC    = SQL_NUMERIC;
static const int DBI_SQL_DECIMAL    = SQL_DECIMAL;
static const int DBI_SQL_INTEGER    = SQL_INTEGER;
static const int DBI_SQL_SMALLINT   = SQL_SMALLINT;
static const int DBI_SQL_FLOAT      = SQL_FLOAT;
static const int DBI_SQL_REAL       = SQL_REAL;
static const int DBI_SQL_DOUBLE     = SQL_DOUBLE;
static const int DBI_SQL_DATE       = SQL_DATE;
static const int DBI_SQL_TIME       = SQL_TIME;
static const int DBI_SQL_TIMESTAMP  = SQL_TIMESTAMP;
static const int DBI_SQL_VARCHAR    = SQL_VARCHAR;

/* conflicts */

#undef	SQL_CHAR
#undef	SQL_NUMERIC
#undef	SQL_DECIMAL
#undef	SQL_INTEGER
#undef	SQL_SMALLINT
#undef	SQL_FLOAT
#undef	SQL_REAL
#undef	SQL_DOUBLE
#undef  SQL_DATE
#undef  SQL_TIME
#undef  SQL_TIMESTAMP
#undef	SQL_VARCHAR

#include <ibase.h>

/* defines */

#define IB_ALLOC_FAIL 	2	
#define IB_FETCH_ERROR	1

#ifndef ISC_STATUS_LENGTH
#define ISC_STATUS_LENGTH 20
#endif

#ifndef SvPV_nolen
# define SvPV_nolen(sv) SvPV(sv, na)
#endif

#define DPB_FILL_BYTE(dpb, byte)			\
    {							\
	*dpb = byte;				\
	dpb += 1;					\
    }

/*
 * 2001-02-13 - Mike Shoyher: It's illegal to assign int to char pointer
 * on platforms where strict aligment is required (like SPARC or m68k). For
 * x86 it isn't important.
 * *((int *)dpb) = isc_vax_integer((char *) &integer, 4); \
 */

#define DPB_FILL_INTEGER(dpb, integer)			\
    {							\
	int tmp;					\
	*(dpb) = 4;					\
	dpb += 1;					\
    tmp = isc_vax_integer((char *) &integer, 4); \
    memcpy(dpb,&tmp,sizeof(tmp));                \
	dpb += 4;					\
    }
#define DPB_FILL_STRING(dpb, string)			\
    {							\
	char l = strlen(string) & 0xFF;			\
	*(dpb) = l;					\
	dpb += 1;					\
	strncpy(dpb, string, (size_t) l);		\
	dpb += l;					\
    }

#define BLOB_SEGMENT	    (256)
#define MAX_BLOB_SEGMENT    (80)
#define DEFAULT_SQL_DIALECT (1)
#define INPUT_XSQLDA        (1)
#define OUTPUT_XSQLDA       (0)

#define SUCCESS		        (0)
#define FAILURE		        (-1)

/*
 * Hardcoded limit on the length of a Blob that can be fetched into a scalar.
 * If you want to fetch Blobs that are bigger, write your own Perl
 */

#define MAX_SAFE_BLOB_LENGTH (1000000)

/****************/
/* data types   */
/****************/

/* XXX not used yet 
typedef struct imp_fbh_st imp_fbh_t;
*/

struct imp_drh_st {
	dbih_drc_t com;     /* MUST be first element in structure   */
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
	dbih_dbc_t 	com;                /* MUST be first element in structure   */
    isc_db_handle	db;
	isc_tr_handle	tr;
	int				init_commit;	/* takes DBD::Pg's route */
    char ISC_FAR    *tpb_buffer;	/* transaction parameter buffer */
    unsigned short	tpb_length;		/* length of tpb_buffer */
    unsigned short	sqldialect;		/* default sql dialect */
};

/* Define sth implementor data structure */
struct imp_sth_st {
	dbih_stc_t  com;                /* MUST be first element in structure   */
    isc_stmt_handle	stmt;
    XSQLDA *	out_sqlda;			/* for storing select-list items */
    XSQLDA *	in_sqlda;			/* for storing placeholder values */
    char *		cursor_name;
	long		type; 				/* statement type */
	int			done_desc;			/* is the statement already dbd_describe()-ed? */
	char		count_item;
	int			fetched;			/* number of fetched rows */
	char ISC_FAR    *ib_dateformat;
	char ISC_FAR    *ib_timestampformat;
	char ISC_FAR    *ib_timeformat;
};

typedef struct vary {
    short          vary_length;
    char           vary_string [1];
} VARY;

#define dbd_init            ib_init
#define dbd_db_login        ib_db_login
#define dbd_db_do           ib_db_do
#define dbd_db_commit       ib_db_commit
#define dbd_db_rollback     ib_db_rollback
#define dbd_db_disconnect   ib_db_disconnect
#define dbd_db_destroy      ib_db_destroy
#define dbd_db_STORE_attrib ib_db_STORE_attrib
#define dbd_db_FETCH_attrib ib_db_FETCH_attrib
#define dbd_st_prepare      ib_st_prepare
#define dbd_st_rows         ib_st_rows
#define dbd_st_execute      ib_st_execute
#define dbd_st_fetch        ib_st_fetch
#define dbd_st_finish       ib_st_finish
#define dbd_st_destroy      ib_st_destroy
#define dbd_st_blob_read    ib_st_blob_read
#define dbd_st_STORE_attrib ib_st_STORE_attrib
#define dbd_st_FETCH_attrib ib_st_FETCH_attrib
#define dbd_describe        ib_describe
#define dbd_bind_ph         ib_bind_ph

void    do_error _((SV *h, int rc, char *what));
/* void    fbh_dump _((imp_fbh_t *fbh, int i)); */

void    dbd_init _((dbistate_t *dbistate));
void    dbd_preparse _((SV *sth, imp_sth_t *imp_sth, char *statement));
int     dbd_describe _((SV* sth, imp_sth_t *imp_sth));

int ib_start_transaction(SV *h, imp_dbh_t *imp_dbh, char *tpb, unsigned short len);
int ib_commit_transaction(SV *h, imp_dbh_t *imp_dbh);

SV* dbd_db_quote(SV* dbh, SV* str, SV* type);
/* end */

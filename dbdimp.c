/* 
   $Id: dbdimp.c,v 1.12 2000/08/28 11:00:33 edpratomo Exp $ 

   Copyright (c) 1999,2000  Edwin Pratomo

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.

*/

#include "InterBase.h"

DBISTATE_DECLARE;

#define IB_SQLtimeformat(format, svp) \
{ \
    STRLEN len; \
    char *buf = SvPV(*svp, len); \
    if ((format = (char*)safemalloc(sizeof(char)* (len +1))) == NULL) \
    { \
        do_error(sth, 2, "Can't alloc SQL time format"); \
        return FALSE; \
    } \
    strcpy(format, buf); \
}

#define IB_alloc_sqlda(sqlda, n) \
{                               \
    short len = n;              \
    if (sqlda) {                \
        safefree(sqlda);sqlda = NULL;       \
    }                           \
    if (!(sqlda = (XSQLDA*) safemalloc(XSQLDA_LENGTH(len)))) { \
        do_error(sth, 2, "Fail to allocate XSQLDA");    \
    } \
    sqlda->sqln = len; \
    sqlda->version = SQLDA_VERSION1; \
}

int create_cursor_name(SV *sth, imp_sth_t *imp_sth)
{
    ISC_STATUS status[ISC_STATUS_LENGTH];
    if ((imp_sth->cursor_name = (char *) safemalloc(22)) == NULL)
    {
        do_error(sth, IB_ALLOC_FAIL, "Cannot allocate cursor name.");
        return FALSE;
    }

    sprintf(imp_sth->cursor_name, "perl%016.16x", imp_sth->stmt);
    isc_dsql_set_cursor_name(status, &(imp_sth->stmt),
    imp_sth->cursor_name, (unsigned short) NULL);
    if (ib_error_check(sth, status))
        return FALSE;
    else 
        return TRUE;
}

void* dump_str(char *src, int len) 
{
    int i;
    fprintf(DBILOGFP, "Dumping string: ");
    for (i = 0; i < len; i++) {
        fputc(src[i], DBILOGFP);
    }
}

int dbd_describe(SV* sth, imp_sth_t *imp_sth)
{
    ISC_STATUS status[ISC_STATUS_LENGTH];
    isc_dsql_describe(status, &(imp_sth->stmt), 1, imp_sth->out_sqlda);
    if (ib_error_check(sth, status))
        return FALSE;
    imp_sth->done_desc = 1;
    return TRUE;
}

void dbd_init(dbistate_t *dbistate)
{
    DBIS = dbistate;
}

void ib_cleanup_st_prepare (imp_sth_t *imp_sth)
{
    if (imp_sth->in_sqlda) {
        safefree(imp_sth->in_sqlda);
        imp_sth->in_sqlda = NULL;
    }
    if (imp_sth->out_sqlda) {
        safefree(imp_sth->out_sqlda);
        imp_sth->out_sqlda = NULL;
    }
    if (imp_sth->done_desc)
        imp_sth->done_desc = 0;

    if (imp_sth->ib_dateformat) {
        safefree(imp_sth->ib_dateformat);
        imp_sth->ib_dateformat = NULL;
    }

    if (imp_sth->ib_timeformat) {
        safefree(imp_sth->ib_timeformat);
        imp_sth->ib_timeformat = NULL;
    }

    if (imp_sth->ib_timestampformat) {
        safefree(imp_sth->ib_timestampformat);
        imp_sth->ib_timestampformat = NULL;
    }
}

void ib_cleanup_st_execute (imp_sth_t *imp_sth) 
{
    if (imp_sth->in_sqlda)
    {
        int i;
        XSQLVAR *var = imp_sth->in_sqlda->sqlvar;
        for (i = imp_sth->in_sqlda->sqln; i > 0; i--, var++)
        {
            safefree(var->sqldata);
            if (var->sqlind)
                *(var->sqlind) = -1;    /* isNULL */
        }
    }
}

/* lower level error handling */
void do_error(SV *h, int rc, char *what)
{
    D_imp_xxh(h);
    SV *errstr = DBIc_ERRSTR(imp_xxh);
    sv_setiv(DBIc_ERR(imp_xxh), (IV)rc);
    sv_setpv(errstr, what);
    DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr);

    if (dbis->debug >= 2)
    PerlIO_printf(DBILOGFP, "%s error %d recorded: %s\n",
        what, rc, SvPV(errstr,na));
}

/* higher level error handling, check and decode status */
int ib_error_check(SV *h, ISC_STATUS *status) 
{
    long sqlcode;
    char msg[1024], *pmsg;

    ISC_STATUS *pvector = status;
    pmsg = msg;
        
    if (status[0] == 1 && status[1] > 0) {
        /* isc_interprete(msg, &pvector); */

        /* isc_sql_interprete doesn't work as expected */
        /* isc_sql_interprete(sqlcode, msg, 1024); */

        if ((sqlcode = isc_sqlcode(status)) != 0) {
            isc_sql_interprete(sqlcode, pmsg, 1024);
            while (*pmsg) pmsg++;
            *pmsg++ = '\n';
            *pmsg++ = '-';
        }

        while (isc_interprete(pmsg, &pvector)) {
            while (*pmsg) pmsg++;
            *pmsg++ = '\n';
            *pmsg++ = '-';
        }
        *--pmsg = '\0'; 

        do_error(h, sqlcode, msg);
        return FAILURE;
    } else return SUCCESS;
}

static int ib2sql_type(int ibtype)
{
    switch(ibtype) {    /* InterBase Internal (not external) types */
    case SQL_TEXT:
    case 453:
        return DBI_SQL_CHAR;
    case SQL_LONG:
    case 497:
        return DBI_SQL_INTEGER;  /* integer */
    case SQL_SHORT:
    case 501:
        return DBI_SQL_SMALLINT; /* smallint */
    case SQL_FLOAT:
    case 483:
        return DBI_SQL_FLOAT;
    case SQL_DOUBLE:
    case 481:
        return DBI_SQL_DOUBLE;
    case SQL_TYPE_DATE:
    case 571:
        return DBI_SQL_DATE;
    case SQL_TYPE_TIME: 
    case 561:
        return DBI_SQL_TIME;
    case SQL_TIMESTAMP:
    case 511:
        return DBI_SQL_TIMESTAMP;
    case SQL_VARYING:
    case 449:
        return DBI_SQL_VARCHAR;
    }
    /* else map type into DBI reserved standard range */
    return -9000 - ibtype;
}

/* from DBI (ANSI/ISO/ODBC) types to InterBase types */
static int ib_sql_type(imp_sth_t *imp_sth, char *name, int sql_type)
{
    /* XXX should detect DBI reserved standard type range here */
/*
    switch (sql_type) {
    case DBI_SQL_NUMERIC:
    case DBI_SQL_DECIMAL:
    case DBI_SQL_INTEGER:
    case DBI_SQL_BIGINT:
    case DBI_SQL_TINYINT:
    case DBI_SQL_SMALLINT:
    case DBI_SQL_FLOAT:
    case DBI_SQL_REAL:
    case DBI_SQL_DOUBLE:
        return 481;     
    case DBI_SQL_VARCHAR:
        return 449; 
    case DBI_SQL_CHAR:
        return 453; 
    
    case SQL_DATE:
    case SQL_TIME:
    case SQL_TIMESTAMP:
    default:
    if (DBIc_WARN(imp_sth) && imp_sth && name)
        warn("SQL type %d for '%s' is not fully supported, bound as SQL_VARCHAR instead");
    return ib_sql_type(imp_sth, name, DBI_SQL_VARCHAR);
    }
*/
}

int dbd_db_login(SV *dbh, imp_dbh_t *imp_dbh, char *dbname, char  *uid, 
    char *pwd)
{
    return dbd_db_login6(dbh, imp_dbh, dbname, uid, pwd, Nullsv);
}

int dbd_db_login6(SV *dbh, imp_dbh_t *imp_dbh, char *dbname, char *uid, 
    char *pwd, SV *attr)
{
    dTHR;
    D_imp_drh_from_dbh;

    ISC_STATUS status[ISC_STATUS_LENGTH];

    HV *hv;
    SV *sv;
    SV **svp;               /* versatile scalar storage */
    unsigned short ib_dialect, ib_cache;
    char *ib_charset, *ib_role;
    char ISC_FAR *dpb_buffer, *dpb;

    char ISC_FAR *user;
    char ISC_FAR *password;
    char ISC_FAR *database;
    char ISC_FAR *host;

    STRLEN len, lna; /* for SvPV */  

    bool dbkey_scope = 1;
    short dpb_length = 0;
    imp_dbh->db = 0L;
    imp_dbh->tr = 0L;
        
    /* 
     * Parse DSN and init values 
     */
    sv = DBIc_IMP_DATA(imp_dbh);
    
    if (!sv  ||  !SvROK(sv)) 
        return FALSE;
    
    hv = (HV*) SvRV(sv);
    if (SvTYPE(hv) != SVt_PVHV) 
        return FALSE;
    
    if ((svp = hv_fetch(hv, "host", 4, FALSE))) {
        host = SvPV(*svp, len);
        if (!len) {
            host = NULL;
        }
    } else 
        host = NULL;

    if ((svp = hv_fetch(hv, "user", 4, FALSE))) {
        user = SvPV(*svp, len);
        if (!len) {
            user = NULL;
        }
    } else 
        user = NULL;
    
    if ((svp = hv_fetch(hv, "password", 8, FALSE))) {
        password = SvPV(*svp, len);
        if (!len) {
            password = NULL;
        }
    } else 
        password = NULL;
    
    if ((svp = hv_fetch(hv, "database", 8, FALSE))) {
        database = SvPV(*svp, lna);
    } else 
        database = NULL;
    
    /* 
     * role, cache, charset, sqldialect 
     */
    if ((svp = hv_fetch(hv, "ib_dialect", 10, FALSE))) {
        ib_dialect = SvIV(*svp);
    } else {
        ib_dialect = DEFAULT_SQL_DIALECT;
    }

    if ((svp = hv_fetch(hv, "ib_cache", 8, FALSE))) {
        ib_cache = SvIV(*svp);
    } else {
        ib_cache = 0;
    }

    if ((svp = hv_fetch(hv, "ib_charset", 10, FALSE))) {
        ib_charset = SvPV(*svp, lna);
    } else 
        ib_charset = NULL;
        
    if ((svp = hv_fetch(hv, "ib_role", 6, FALSE))) {
        ib_role = SvPV(*svp, lna);
    } else 
        ib_role = NULL;

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_db_login6\n");

    /* 
     * Allocate DPB 
     */
    if ((dpb_buffer = (char*) safemalloc(1024 * sizeof(char))) == NULL) 
    {
        do_error(dbh, 2, "Not enough memory to allocate DPB");
        return FALSE;
    }

    /* 
     * Allocate TPB 
     */
    if ((imp_dbh->tpb_buffer = (char*) safemalloc(10 * sizeof(char))) == NULL) 
    {
        do_error(dbh, 2, "Not enough memory to allocate TPB");
        return FALSE;
    }

    /* I'm not sure about this.
    Default SQL dialect for every statement  */

    imp_dbh->sqldialect = ib_dialect;

    /* 
     * Fill DPB 
     */
    dpb = dpb_buffer;
    DPB_FILL_BYTE(dpb, isc_dpb_version1);

    DPB_FILL_BYTE(dpb, isc_dpb_user_name);
    DPB_FILL_STRING(dpb, uid);

    DPB_FILL_BYTE(dpb, isc_dpb_password);
    DPB_FILL_STRING(dpb, pwd);


    if (ib_cache)
    {
        /*
         * Safety check: Do not allocate a cache buffer greater than
         * 10000 pages, so we don't exhaust memory inadvertently.
         */
        if (ib_cache > 10000) ib_cache = 10000;
        DPB_FILL_BYTE(dpb, isc_dpb_num_buffers);
        DPB_FILL_INTEGER(dpb, ib_cache);
    }

    DPB_FILL_BYTE(dpb, isc_dpb_SQL_dialect);
    DPB_FILL_INTEGER(dpb, ib_dialect);
    
    DPB_FILL_BYTE(dpb, isc_dpb_dbkey_scope);
    DPB_FILL_INTEGER(dpb, dbkey_scope);
    
    if (ib_charset) 
    {
        DPB_FILL_BYTE(dpb, isc_dpb_lc_ctype);
        DPB_FILL_STRING(dpb, ib_charset);
    }

    if (ib_role) 
    {
        DPB_FILL_BYTE(dpb, isc_dpb_sql_role_name);
        DPB_FILL_STRING(dpb, ib_role);
    }

    dpb_length = dpb - dpb_buffer;
    
    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "dbd_db_login6: attaching to database %s..\n",
        database);

    isc_attach_database(
        status,             /* status vector */
        0,                  /* connect string is null-terminated */
        database,           /* connect string */
        &(imp_dbh->db),     /* ref to db handle */
        dpb_length,         /* length of dpb */
        dpb_buffer);        /* connect options */
    
    if (ib_error_check(dbh, status))
        return FALSE;

    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "dbd_db_login6: success attaching.\n");

    imp_dbh->init_commit = 1;
    /* transaction initialization code is moved into ib_start_transaction() */  

    /*
     *  Tell DBI, that dbh->disconnect should be called for this handle
     */
    DBIc_ACTIVE_on(imp_dbh);

    /*
     *  Tell DBI, that dbh->destroy should be called for this handle
     */
    DBIc_on(imp_dbh, DBIcf_IMPSET);

    return TRUE;
}

int dbd_db_disconnect(SV *dbh, imp_dbh_t *imp_dbh)
{
    dTHR;
    D_imp_drh_from_dbh;
    ISC_STATUS status[ISC_STATUS_LENGTH];

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_db_disconnect\n");

    DBIc_ACTIVE_off(imp_dbh);

    /* rollback if AutoCommit = off */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit) == FALSE) {

        /* rollback and close trans context */
        isc_rollback_transaction(status, &(imp_dbh->tr));
        if (ib_error_check(dbh, status))
            return FALSE;
    } else {
        /* if AutoCommit On, proceed with InterBase's default behavior */
        
        /* commit and close transaction context 
        isc_commit_transaction(status, &(imp_dbh->tr));
        if (ib_error_check(dbh, status))
            return FALSE;
        */
    }

    isc_detach_database(status, &(imp_dbh->db));
    if (ib_error_check(dbh, status))
        return FALSE;

    return TRUE;
}

void dbd_db_destroy (SV *dbh, imp_dbh_t *imp_dbh) {
    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_db_destroy\n");
        
    if (DBIc_ACTIVE(imp_dbh)) {
        dbd_db_disconnect(dbh, imp_dbh);
    }
                        
    /* Nothing in imp_dbh to be freed   */
    DBIc_IMPSET_off(imp_dbh);
}

int dbd_db_commit (SV *dbh, imp_dbh_t *imp_dbh) 
{
    ISC_STATUS status[ISC_STATUS_LENGTH];
    
    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_db_commit\n");

/* XXX still have problem with this */
    /* no commit if AutoCommit on */

    if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) 
        return FALSE;

    /* if AutoCommit = off, execute commit without closing transaction 
     * context 
     */
    isc_commit_retaining(status, &(imp_dbh->tr));
    if (ib_error_check(dbh, status)) 
        return FALSE;

    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "isc_commit_retaining succeed.\n");

    return TRUE;
}

int dbd_db_rollback(SV *dbh, imp_dbh_t *imp_dbh) 
{
    dTHR;
    ISC_STATUS status[ISC_STATUS_LENGTH];

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_db_rollback\n");
    
    /* no rollback if AutoCommit = on */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit) != FALSE) {
        return FALSE;
    }

    isc_rollback_transaction(status, &(imp_dbh->tr));
    if (ib_error_check(dbh, status))
        return FALSE;

    /* start new trans */
    isc_start_transaction(status, &(imp_dbh->tr), 1, &(imp_dbh->db),
        imp_dbh->tpb_length, imp_dbh->tpb_buffer );

    if (ib_error_check(dbh, status))
        return FALSE;

    return TRUE;    
}

int dbd_db_STORE_attrib(SV *dbh, imp_dbh_t *imp_dbh, SV *keysv, SV *valuesv)
{
    STRLEN  kl;
    ISC_STATUS status[ISC_STATUS_LENGTH];
    char *key = SvPV(keysv, kl);
    int on = SvTRUE(valuesv);
    int oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit);
    
    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_db_STORE - %s\n", key);

    if ((kl==10) && strEQ(key, "AutoCommit"))
    {
        DBIc_set(imp_dbh, DBIcf_AutoCommit, on);
        if (oldval == FALSE && on && imp_dbh->init_commit) 
        /* AutoCommit set from 0 to 1 */
        {
            /* do nothing, fall through */
            if (dbis->debug >= 3) 
            { 
                PerlIO_printf(DBILOGFP, 
                    "dbd_db_STORE: initialize AutoCommit to on\n"); 
            }
        } else if (oldval == FALSE && on) {
            /* AutoCommit set from 0 to 1 */
            /* commit any outstanding changes */

            if (!ib_commit_transaction(dbh, imp_dbh))
                return FALSE;

            if (dbis->debug >= 3) { 
                PerlIO_printf(DBILOGFP, 
                    "dbd_db_STORE: switch AutoCommit to on: commit\n"); 
            }
        } else if ((oldval != FALSE && !on) || (oldval == FALSE && !on && imp_dbh->init_commit)) {
            /* AutoCommit set from 1 to 0 */
            /* AutoCommit set from 0 to 0 */

            /* start a new transaction using default TPB */
            if (!ib_start_transaction(dbh, imp_dbh, NULL, 0)) 
                croak("Error starting default transaction\n");
                
            if (dbis->debug >= 3) { 
                PerlIO_printf(DBILOGFP, 
                "dbd_db_STORE: switch AutoCommit to off: begin\n"); 
            }
        }
        /* only needed once */
        imp_dbh->init_commit = 0;
        return TRUE; /* handled */
    } 
    return FALSE; /* not handled */
}

SV *dbd_db_FETCH_attrib(SV *dbh, imp_dbh_t *imp_dbh, SV *keysv)
{
    STRLEN  kl;
    char *  key = SvPV(keysv, kl);
    SV *    result = NULL;

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_db_FETCH - %s\n", key);

    if ((kl==10) && (strcmp(key, "AutoCommit") == 0))
    {
        result = boolSV(DBIc_is(imp_dbh, DBIcf_AutoCommit));
    }

    if (result == NULL)
        return Nullsv;
    else
        if ((result == &sv_yes) || (result == &sv_no))
            return result;
        else
            return sv_2mortal(result);
}

void dbd_preparse(SV *sth, imp_sth_t *imp_sth, char *statement)
{
    ISC_STATUS status[ISC_STATUS_LENGTH];

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "Enter dbd_preparse\n");

    isc_dsql_describe_bind(status, &(imp_sth->stmt), 1,
        imp_sth->in_sqlda);
    if (ib_error_check(sth, status)) {
        ib_cleanup_st_prepare(imp_sth);
        return;
    }

    /* realloc in_sqlda and rebind if not enough XSQLVAR for bind params */
    if (imp_sth->in_sqlda->sqld > imp_sth->in_sqlda->sqln) 
    {
        IB_alloc_sqlda(imp_sth->in_sqlda, imp_sth->in_sqlda->sqld);
        if (imp_sth->in_sqlda == NULL) 
        {
            do_error(sth, 1, "Fail to reallocate in_slqda");
            ib_cleanup_st_prepare(imp_sth);
            return;
        } else {
            isc_dsql_describe_bind(status, &(imp_sth->stmt), 1,
                imp_sth->in_sqlda);
            if (ib_error_check(sth, status)) {
                ib_cleanup_st_prepare(imp_sth);
                return;
            }
        }
    }
    if (dbis->debug >= 3) 
    {
        PerlIO_printf(DBILOGFP, "dbd_preparse: describe_bind passed.\n");
        PerlIO_printf(DBILOGFP, 
        "in_sqlda: sqld: %d, sqln: %d.\n", imp_sth->in_sqlda->sqld, 
        imp_sth->in_sqlda->sqln);
    }

    DBIc_NUM_PARAMS(imp_sth) = imp_sth->in_sqlda->sqld;
    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "Exiting dbd_preparse\n");
}

int dbd_st_prepare(SV *sth, imp_sth_t *imp_sth, char *statement, SV *attribs)
{
    D_imp_dbh_from_sth;
    ISC_STATUS status[ISC_STATUS_LENGTH];
    int i;
    short dtype;
    static char stmt_info[1];
    char info_buffer[20], count_item;
    XSQLVAR *var;
        
    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "Enter dbd_st_prepare\n");

    if (!DBIc_ACTIVE(imp_dbh)) {
        do_error(sth, -1, "Database disconnected");
        return FALSE;
    }

    /* init values */
    count_item = 0;

	imp_sth->count_item = 0;	
	imp_sth->fetched = -1;
    imp_sth->done_desc = 0;
    imp_sth->in_sqlda = NULL;
    imp_sth->out_sqlda = NULL;
    imp_sth->cursor_name = NULL;

    imp_sth->ib_dateformat = NULL;
    imp_sth->ib_timestampformat = NULL;
    imp_sth->ib_timeformat = NULL;

    if (attribs) {
        SV **svp;
        if ((svp = DBD_ATTRIB_GET_SVP(attribs, "ib_dateformat", 13)) != NULL)
            IB_SQLtimeformat(imp_sth->ib_dateformat, svp);
        if ((svp = DBD_ATTRIB_GET_SVP(attribs, "ib_timestampformat", 18)) != NULL)
            IB_SQLtimeformat(imp_sth->ib_timestampformat, svp);
        if ((svp = DBD_ATTRIB_GET_SVP(attribs, "ib_timeformat", 13)) != NULL)
            IB_SQLtimeformat(imp_sth->ib_timeformat, svp);
    }


    /* allocate 1 XSQLVAR to in_sqlda */
    IB_alloc_sqlda(imp_sth->in_sqlda, 1);
    if (imp_sth->in_sqlda == NULL)
    {
        do_error(sth, 2, "Fail to allocate in_sqlda");
        return FALSE;
    }
    
    if (dbis->debug >= 3) {
        if (imp_sth->in_sqlda == NULL) 
           PerlIO_printf(DBILOGFP, "dbd_st_prepare: in_sqlda null.\n");
        else 
           PerlIO_printf(DBILOGFP, "dbd_st_prepare: in_sqlda OK.\n");
    } 

    /* allocate 1 XSQLVAR to out_sqlda */
    IB_alloc_sqlda(imp_sth->out_sqlda, 1);
    if (imp_sth->out_sqlda == NULL)
    {
        do_error(sth, 2, "Fail to allocate out_sqlda");
        ib_cleanup_st_prepare(imp_sth);
        return FALSE;
    }

    if (dbis->debug >= 3) {
        if (imp_sth->out_sqlda == NULL) 
           PerlIO_printf(DBILOGFP, "dbd_st_prepare: out_sqlda null.\n");
        else 
           PerlIO_printf(DBILOGFP, "dbd_st_prepare: out_sqlda OK.\n");
    } 

    /* init statement handle */
    isc_dsql_alloc_statement2(status, &(imp_dbh->db), &(imp_sth->stmt));
    if (ib_error_check(sth, status)) {
        ib_cleanup_st_prepare(imp_sth);     
        return FALSE;
    }

    if (dbis->debug >= 3) 
       PerlIO_printf(DBILOGFP, 
       "dbd_st_prepare: sqldialect: %d.\n", imp_dbh->sqldialect);

/* XXX add handling if AutoCommit On, where imp_dbh->tr = 0L */
    /* AutoCommit = on */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
        /* start a new transaction using default TPB */
        if (!ib_start_transaction(sth, imp_dbh, NULL, 0)) {
            ib_cleanup_st_prepare(imp_sth);
            return FALSE;
        }
    }

/* XXX strange, fail for "" */

    if (dbis->debug >= 3) 
       PerlIO_printf(DBILOGFP, 
       "dbd_st_prepare: statement: %s.\n", statement);

    isc_dsql_prepare(status, &(imp_dbh->tr), &(imp_sth->stmt), 0,
        statement, imp_dbh->sqldialect, imp_sth->out_sqlda);

    if (ib_error_check(sth, status)) {
        ib_cleanup_st_prepare(imp_sth);
        return FALSE;
    }

    if (dbis->debug >= 3)
       PerlIO_printf(DBILOGFP, 
       "dbd_st_prepare: isc_dsql_prepare succeed..\n");

    stmt_info[0] = isc_info_sql_stmt_type;
    isc_dsql_sql_info(status,
        &(imp_sth->stmt),
        sizeof (stmt_info),   stmt_info,
        sizeof (info_buffer), info_buffer);

    if (ib_error_check(sth, status)) {
        ib_cleanup_st_prepare(imp_sth);
        return FALSE;
    }
    {
        short l = (short) isc_vax_integer((char *) info_buffer + 1, 2);
        imp_sth->type = isc_vax_integer((char *) info_buffer + 3, l);
    }

    /* sanity check of statement type */
    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, 
            "dbd_st_prepare: statement type: %ld.\n", imp_sth->type);

    switch (imp_sth->type)
    {
    /*
     * Implemented statement types.
     */
    case isc_info_sql_stmt_select:
    case isc_info_sql_stmt_select_for_upd:
#if 1
        /*
         * Unfortunately, select count item doesn't work
         * in current versions of InterBase. 
         * isql does it literally by fetching everything
         * and counting the number of rows it fetched.
         * InterBase doesn't seem to be able to estimate
         * the number of rows before the client app
         * fetches them all.
         */
        count_item = isc_info_req_select_count;
#endif
        break;

    case isc_info_sql_stmt_insert:
        count_item = isc_info_req_insert_count;
        break;

    case isc_info_sql_stmt_update:
        count_item = isc_info_req_update_count;
        break;

    case isc_info_sql_stmt_delete:
        count_item = isc_info_req_delete_count;
        break;

    case isc_info_sql_stmt_ddl:
        /* no count_item to gather */
        break;

    case isc_info_sql_stmt_exec_procedure:
        /* no count_item to gather */
        break;

    /*
     * Unimplemented statement types.
     * Some may be implemented in the future.
     */
    case isc_info_sql_stmt_get_segment:
    case isc_info_sql_stmt_put_segment:
    case isc_info_sql_stmt_start_trans:
    case isc_info_sql_stmt_commit:
    case isc_info_sql_stmt_rollback:
    default:
        do_error(sth, 10, "Statement type is not implemented in this version of DBD::InterBase");
        return FALSE;
        break;
    }

	imp_sth->count_item = count_item;
	
    /* scan statement for '?', ':1' and/or ':foo' style placeholders    */
    /* realloc in_sqlda where needed */
    dbd_preparse(sth, imp_sth, statement);

    /* Now fill out_sqlda with select-list items */
    if (!dbd_describe(sth, imp_sth)) 
    {
        ib_cleanup_st_prepare(imp_sth);
        return FALSE;
    }

    if (dbis->debug >= 3) 
    {
        PerlIO_printf(DBILOGFP, "dbd_st_prepare: dbd_describe passed.\n");  
        PerlIO_printf(DBILOGFP, 
        "out_sqlda: sqld: %d, sqln: %d. ", imp_sth->out_sqlda->sqld, 
        imp_sth->out_sqlda->sqln);
        PerlIO_printf(DBILOGFP, 
        "done_desc: %d.\n", imp_sth->done_desc);
    }

    /* enough output parameter block ? */
    if (imp_sth->out_sqlda->sqld > imp_sth->out_sqlda->sqln) 
    {
        if (dbis->debug >= 3) 
            PerlIO_printf(DBILOGFP, "dbd_st_prepare: realloc out_sqlda..\n");
        IB_alloc_sqlda(imp_sth->out_sqlda, imp_sth->out_sqlda->sqld);
        if (dbis->debug >= 3) 
        {
            if (imp_sth->out_sqlda == NULL) {
                PerlIO_printf(DBILOGFP, 
                "dbd_st_prepare: realloc out_sqlda failed.\n"); 
            } else 
                PerlIO_printf(DBILOGFP, 
                "dbd_st_prepare: realloc out_sqlda success.\n");    
        }
        if (imp_sth->out_sqlda == NULL) 
        {
            do_error(sth, IB_ALLOC_FAIL, "Fail to reallocate out_sqlda");
            ib_cleanup_st_prepare(imp_sth);
            return FALSE;
        } else {
            if (dbis->debug >= 3) 
            PerlIO_printf(DBILOGFP, 
            "dbd_st_prepare: calling isc_dsql_describe again..\n"); 

            isc_dsql_describe(status, &(imp_sth->stmt), 1, imp_sth->out_sqlda);
            if (ib_error_check(sth, status)) {
                ib_cleanup_st_prepare(imp_sth);
                return FALSE;
            }
            if (dbis->debug >= 3) 
            PerlIO_printf(DBILOGFP, 
            "dbd_st_prepare: success calling isc_dsql_describe.\n");    
        }
    }
    else
    if (imp_sth->out_sqlda->sqld == 0) /* not a select statement */
    {
        safefree(imp_sth->out_sqlda);
        imp_sth->out_sqlda = NULL;
    }

    if (imp_sth->out_sqlda) {
        for (i = 0, var = imp_sth->out_sqlda->sqlvar; 
            i < imp_sth->out_sqlda->sqld;
            i++, var++) 
        {
            dtype = (var->sqltype & ~1);

            /* specify time format, if needed */

            switch (dtype) {
            case SQL_TYPE_DATE:
                if (imp_sth->ib_dateformat == NULL) { 
                    if ((imp_sth->ib_dateformat = 
                        (char *)safemalloc(sizeof(char) * 3)) == NULL)
                    {
                        do_error(sth, 2, "Can't alloc ib_dateformat.\n");
                        return FALSE;
                    }   
                    strcpy(imp_sth->ib_dateformat, "%x");
                }
                break;
            case SQL_TIMESTAMP:
                if (imp_sth->ib_timestampformat == NULL) {
                    if ((imp_sth->ib_timestampformat = 
                        (char *)safemalloc(sizeof(char) * 3)) == NULL)
                    {
                        do_error(sth, 2, "Can't alloc ib_timestampformat.\n");
                        return FALSE;
                    }   
                    strcpy(imp_sth->ib_timestampformat, "%c");
                }
                break;
            
            case SQL_TYPE_TIME:
                if (imp_sth->ib_timeformat == NULL) {
                    if ((imp_sth->ib_timeformat = 
                        (char *)safemalloc(sizeof(char) * 3)) == NULL)
                    {
                        do_error(sth, 2, "Can't alloc ib_timeformat.\n");
                        return FALSE;
                    }
                    strcpy(imp_sth->ib_timeformat, "%X");
                }
                break;
            }
            
            /* Alloc space for sqldata */
            var->sqldata = (char *) safemalloc( var->sqllen +
                (dtype == SQL_VARYING ? sizeof(short) : 0) );
                if ( ! var->sqldata)
            {
                do_error(sth, 2, "Cannot allocate XSQLDA sqldata.\n");
                return FALSE;
            }
        
            /* Nullable? */
            if (var->sqltype & 1)
                if ((var->sqlind = (short*)safemalloc(sizeof(short))) == NULL) 
                {
                    do_error(sth, 2, "Cannot allocate XSQLDA sqlind.\n");
                    return FALSE;
                }
        }
    }
    DBIc_IMPSET_on(imp_sth);

    return TRUE;
}

int dbd_st_execute(SV *sth, imp_sth_t *imp_sth)
{
    D_imp_dbh_from_sth;
    ISC_STATUS  status[20], fetch;     
    char stmt_info[1];
    char info_buffer[20];
    int         result = -2;    
    int row_count;
    
    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_st_execute\n");

/* For AutoCommit On:
 * we can't assume imp_dbh->tr already allocated in prepare() 
 * because execute() gets called repeatedly without re-prepare()
 * for typical usages of bind_param()
 */

    if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
        if (!imp_dbh->tr) {
            if (!ib_start_transaction(sth, imp_dbh, NULL, 0)) 
                return result;
        }
    } 

    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, 
        "dbd_st_execute: statement type: %ld.\n", imp_sth->type);

    if (imp_sth->type == isc_info_sql_stmt_exec_procedure)
    {
        isc_dsql_execute2(status, &(imp_dbh->tr), &(imp_sth->stmt),
        imp_dbh->sqldialect, imp_sth->in_sqlda->sqld > 0 ? 
        imp_sth->in_sqlda: NULL,
        imp_sth->out_sqlda->sqld > 0 ? imp_sth->out_sqlda: NULL);
        if (ib_error_check(sth, status)) {
            ib_cleanup_st_execute(imp_sth);
            return result;
        }
    }
    else /* all other types of SQL statements */
    {
        if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "dbd_st_execute: calling isc_dsql_execute..\n");
        isc_dsql_execute(status, &(imp_dbh->tr), &(imp_sth->stmt),
        imp_dbh->sqldialect,
        imp_sth->in_sqlda->sqld > 0 ? imp_sth->in_sqlda: NULL);
        if (ib_error_check(sth, status)) {
            ib_cleanup_st_execute(imp_sth);
            return result;
        }
        if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "dbd_st_execute: isc_dsql_execute succeed.\n");
    }

    /* Jika AutoCommit On, commit_transaction() (bukan retaining), 
     * dan reset imp_dbh->tr == 0L
     * For SELECT statement, commit_transaction() is called after fetch,
     * or within finish()
     */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit) 
        && imp_sth->type != isc_info_sql_stmt_select
        && imp_sth->type != isc_info_sql_stmt_select_for_upd) 
    {
        if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, 
        "dbd_st_execute: calling ib_commit_transaction..\n");
    
        if (!ib_commit_transaction(sth, imp_dbh)) {
            ib_cleanup_st_execute(imp_sth);
            return result;
        }

        if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, 
        "dbd_st_execute: isc_commit_transaction succeed.\n");
    }

    /*
     * Declare a unique cursor for this query
     */

    if (imp_sth->type == isc_info_sql_stmt_select_for_upd)
    {
        /* We free the cursor_name buffer in dbd_st_destroy. */
        if (!create_cursor_name(sth, imp_sth))
        {
            ib_cleanup_st_execute(imp_sth);
            return result;
        }   
    }

    if (dbis->debug >= 3) 
    {
        if (imp_sth->out_sqlda == NULL) 
        PerlIO_printf(DBILOGFP, "dbd_st_execute: out_sqlda null.\n");
        else 
        PerlIO_printf(DBILOGFP, "dbd_st_execute: out_sqlda OK.\n");
    } 

//    fetch = 0;

    switch (imp_sth->type)
    {
        case isc_info_sql_stmt_select:
        case isc_info_sql_stmt_select_for_upd:
        case isc_info_sql_stmt_exec_procedure:
//          fetch = 0;
            DBIc_NUM_FIELDS(imp_sth) = imp_sth->out_sqlda->sqld;
            DBIc_ACTIVE_on(imp_sth);
            break;
    }

	if (imp_sth->count_item) {
	    stmt_info[0] = imp_sth->count_item; /* isc_info_sql_records; */
    	isc_dsql_sql_info(status,
        	&(imp_sth->stmt),
        	sizeof (stmt_info),   stmt_info,
        	sizeof (info_buffer), info_buffer);

		/* perhaps this is a too strong exception */
    	if (ib_error_check(sth, status)) {
        	ib_cleanup_st_execute(imp_sth);
        	return FALSE;
    	}

	    {
    	    short l = (short) isc_vax_integer((char *) info_buffer + 1, 2);
        	row_count = isc_vax_integer((char *) info_buffer + 3, l);
    	}
    }
    
    result = -1;
	/* XXX attempt to return number of affected rows still doesn't work */
    if (dbis->debug >= 3) {
        PerlIO_printf(DBILOGFP, "dbd_st_execute: row count: %d.\n", row_count);
        PerlIO_printf(DBILOGFP, "dbd_st_execute: count_item: %c.\n", 
        	imp_sth->count_item);
    }
    return result; 
}

/* from out_sqlda to AV */
AV *dbd_st_fetch(SV *sth, imp_sth_t *imp_sth)
{
    D_imp_dbh_from_sth;     /* declare imp_dbh from sth */
    ISC_STATUS  fetch = 0;
    ISC_STATUS  status[20]; /* isc api status vector    */
    int         chopBlanks; /* chopBlanks ?             */
    AV *        av;         /* array buffer             */
    XSQLVAR *   var;        /* working pointer XSQLVAR  */
    int         i;          /* loop */
    int val_length;
    short dtype;
    SV *val;

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_st_fetch\n");

    if (!DBIc_ACTIVE(imp_sth))
    {
        do_error(sth, 0, "no statement executing (perhaps you need to call execute first)\n");
        return Nullav;
    }
    
    chopBlanks = DBIc_is(imp_sth, DBIcf_ChopBlanks);
    
    av = DBIS->get_fbav(imp_sth);
    
    /*
     * if it's an execute procedure, we've already got the
     * output from the isc_dsql_execute2() call in IB_execute().
     */
/*
    if (!imp_sth->type == isc_info_sql_stmt_select ||
        !imp_sth->type == isc_info_sql_stmt_select_for_upd)
    {
        return Nullav;
    }
*/
    /* else .. fetch */
    fetch = isc_dsql_fetch(status, &(imp_sth->stmt),
        imp_dbh->sqldialect, imp_sth->out_sqlda);

    if (ib_error_check(sth, status))
        return Nullav;

    /*
     * Code 100 means we've reached the end of the set
     * of rows that the SELECT will return.
     */

    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "fetch: %d\n", fetch);

	if (imp_sth->fetched < 0) 
		imp_sth->fetched = 0;
		
    if (fetch == 100) 
    {
        /* close the cursor */
        isc_dsql_free_statement(status, &(imp_sth->stmt), DSQL_close);
        if (ib_error_check(sth, status)) 
            return Nullav;
        if (dbis->debug >= 3) 
           PerlIO_printf(DBILOGFP, "isc_dsql_free_statement succeed.\n");

        DBIc_ACTIVE_off(imp_sth); /* dbd_st_finish is no longer needed */

        /* if AutoCommit on XXX. what to return if fails? */
        if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) {

            if (!ib_commit_transaction(sth, imp_dbh))
                return Nullav;
                
            if (dbis->debug >= 3) 
               PerlIO_printf(DBILOGFP, 
               "fetch ends: ib_commit_transaction succeed.\n");
        }
        return Nullav;
    } else if (fetch != 0) /* something bad */
    {   do_error(sth, 0, "Fetch error");
        DBIc_ACTIVE_off(imp_sth);
        return Nullav;
    }

    var = imp_sth->out_sqlda->sqlvar;
    for (i=0; i<imp_sth->out_sqlda->sqld; i++, var++)
    {
        SV *sv = AvARRAY(av)[i];
        dtype = var->sqltype & ~1;

        if ((var->sqltype & 1) && (*(var->sqlind) == -1))
        /* if nullable field */
        {
            /* isNULL */
            SvOK_off(sv);
        }
        else
        {
            /*
             * Got a non-null field.  Got to pass it back to the
             * application, which means some datatype dependant code.
             */

            switch (dtype)
            {
            case SQL_SHORT:
                if (var->sqlscale) /* handle NUMERICs */
                {
                    double numeric;

                    numeric = ((double) (*(short *) var->sqldata))
                        / pow(10.0, (double) -var->sqlscale);
                    /* val = newSVnv(numeric); */
                    sv_setnv(sv, numeric);
                }   
                else
                    /* val = newSViv(*(short *) (var->sqldata)); */
                    sv_setiv(sv, *(short *) (var->sqldata));
                break;

            case SQL_LONG:
                if (var->sqlscale) /* handle NUMERICs */
                {
                    double numeric;

                    numeric = ((double) (*(long *) var->sqldata))
                    / pow(10.0, (double) -var->sqlscale);
                    /* val = newSVnv(numeric); */
                    sv_setnv(sv, numeric);
                }
                else
                    /* val = newSViv(*(long *) (var->sqldata)); */
                    sv_setiv(sv, *(long *) (var->sqldata));
                break;
#ifdef SQL_INT64
            case SQL_INT64:
            /*
             * This seemed difficult at first to return
             * a 64-bit scaled numeric to Perl through the
             * SV interface.  But as luck would have it,
             * Perl treats strings and numerics identically.
             * I can return this numeric as a string and
             * nobody has a problem with it.
             */
                {
                    ISC_INT64 q;
                    char buf[25];
                    q = *((ISC_INT64 *) (var->sqldata));
                /*
                 * %Ld is a GNUism and is not portable.
                 * One alternative would be %qd, but this
                 * is BSD 4.4 syntax and is also not
                 * portable.  The printf(3) man page is
                 * not clear on which is the best method.
                 */
                /*
                 * I deliberately use two integers instead
                 * of casting the scaled int64 to a double.
                 * This avoids rounding errors in conversions
                 * to IEEE float, which is the whole reason
                 * for InterBase support of INT64.
                 */
                    sprintf(buf, "%Ld.%Ld",
                    (ISC_INT64) (q / (int)
                    pow(10.0, (double) -var->sqlscale)),
                    (ISC_INT64) (q % (int)
                    pow(10.0, (double) -var->sqlscale)));
                    /* val = newSVpv(buf, 0); */
                    sv_setpvn(sv, buf, strlen(buf));
                }
                break;
#endif

            case SQL_FLOAT:
                sv_setnv(sv, (double)(*(float *) (var->sqldata)));
                break;

            case SQL_DOUBLE:
                if (var->sqlscale) /* handle NUMERICs */
                {
                    double d = *(double *)var->sqldata;
                    short q = -var->sqlscale;

                    sv_setnv(sv, d > 0?
                    floor(d * pow(10.0, (double) q))
                    / pow(10.0, (double) q):
                    ceil(d * pow(10.0, (double) q))
                    / pow(10.0, (double) q));
                }
                else
                    sv_setnv(sv, *(double *) (var->sqldata));
                break;

            case SQL_TEXT:
            /*
             * Thanks to DAM for pointing out that I
             * don't need to null-terminate this
             * buffer, and in fact it's a buffer
             * overrun if I do!
             */
            /* var->sqldata[var->sqllen] = '\0'; */

                if (dbis->debug >= 3)
                    PerlIO_printf(DBILOGFP, 
                    "Fill in TEXT type..\nLength: %d\n", var->sqllen);

                if (chopBlanks && (var->sqllen > 0))
                {
                    short len = var->sqllen;
                    while(((char *)(var->sqldata))[len-1]==' ')
                        len--;
                    sv_setpvn(sv, var->sqldata, len); 
                } else 
/* XXX problem with fixed width CHAR */
                    sv_setpvn(sv, var->sqldata, var->sqllen); 
/*                  sv_setpv(sv, var->sqldata); */
                break;

            case SQL_VARYING:
                {
                    VARY *vary = (VARY *) var->sqldata;
                    char *string;
                    short len;

                    len = vary->vary_length;
                    string = vary->vary_string;
                    sv_setpvn(sv, string, len);
            /* Note that sqllen for VARCHARs is the max length */
                    break;
                }

        /*
         * If user specifies a TimestampFormat, TimeFormat, or
         * DateFormat property of the Statement class, then that
         * string is the format string for strftime().
         *
         * If the user doesn't specify an XxxFormat, then format
         * is %c, defined in /usr/lib/locale/<locale>/LC_TIME/time,
         * where <locale> is the host's chosen locale.
         */
#ifndef SQL_TIMESTAMP
            case SQL_DATE:
#else
            case SQL_TIMESTAMP:
            case SQL_TYPE_DATE:
            case SQL_TYPE_TIME:
#endif
            {
                char *format, format_buf[20];
                unsigned int len;
                struct tm times;
#ifndef SQL_TIMESTAMP
                isc_decode_date((ISC_QUAD *) var->sqldata, &times);
#else
                switch (dtype)
                {
                case SQL_TIMESTAMP:
                    isc_decode_timestamp((ISC_TIMESTAMP *) var->sqldata, &times);
                    break;
                case SQL_TYPE_DATE:
                    isc_decode_sql_date((ISC_DATE *) var->sqldata, &times);
                    break;
                case SQL_TYPE_TIME:
                    isc_decode_sql_time((ISC_TIME *) var->sqldata, &times);
                    break;
                }
                if (dbis->debug >= 3) 
                    PerlIO_printf(DBILOGFP, 
                    "Decode passed.\n");
#endif
#ifdef __FreeBSD__
            /*
             * FreeBSD has a bug with European time zones
             * This is how they corrected it in POSIX.xs,
             * so I'll do the same here.
             * Thanks to German Myzovsky <lawyer@alpha.tario.ru>
             * for bringing this to my attention.
             */
                times.tm_gmtoff = 0;
                times.tm_zone = "???";
#else /* __FreeBSD__ */
            /* normalize */
                (void) mktime(&times);
#endif /* __FreeBSD__ */

                /* I'm assumming these ib_*formats are null terminated */
#ifndef SQL_TIMESTAMP

                if (strlen(imp_sth->ib_dateformat) + 1 > 20) {
                    do_error(sth, 1, "Buffer overflow.");
                    return FALSE;
                } else
                    strcpy(format_buf, imp_sth->ib_dateformat);
#else
    if (dbis->debug >= 3) {
        PerlIO_printf(DBILOGFP, "ib_dateformat: %s.\n", imp_sth->ib_dateformat);
        PerlIO_printf(DBILOGFP, "Len: %d.\n", strlen(imp_sth->ib_dateformat));
    }

                if (imp_dbh->sqldialect == 1)
                {
                    if (strlen(imp_sth->ib_dateformat) + 1 > 20) {
                        do_error(sth, 1, "Buffer overflow.");
                        return FALSE;
                    } else
                        strcpy(format_buf, imp_sth->ib_dateformat);
                } else switch (dtype)
                {
                    case SQL_TIMESTAMP:
                        if (strlen(imp_sth->ib_timestampformat) + 1 > 20) 
                        {
                            do_error(sth, 1, "Buffer overflow.");
                            return FALSE;
                        } else
                            strcpy(format_buf, imp_sth->ib_timestampformat);
                        break;
                    case SQL_TYPE_DATE:
                        if (strlen(imp_sth->ib_dateformat) + 1 > 20) 
                        {
                            do_error(sth, 1, "Buffer overflow.");
                            return FALSE;
                        } else
                            strcpy(format_buf, imp_sth->ib_dateformat);
                        break;
                    case SQL_TYPE_TIME:
                        if (strlen(imp_sth->ib_timeformat) + 1 > 20) {
                            do_error(sth, 1, "Buffer overflow.");
                            return FALSE;
                        } else
                            strcpy(format_buf, imp_sth->ib_timeformat);
                        break;
                }
#endif
                if (!strlen(format_buf)) 
                {
                /*
                 * Assign defaults in the absence of a
                 * value for the statement property. 
                 */
#ifndef SQL_TIMESTAMP
                    strcpy(format_buf, "%c");
#else
                    switch (dtype)
                    {
                        case SQL_TIMESTAMP:
                            strcpy(format_buf, "%c");
                            break;
                        case SQL_TYPE_DATE:
                            strcpy(format_buf, "%x");
                            break;
                        case SQL_TYPE_TIME:
                            strcpy(format_buf, "%X");
                            break;
                    }
#endif
                }
                format = format_buf;
                {
                    char buf[100];
                    strftime(buf, sizeof(buf), format, &times);
                    sv_setpvn(sv, buf, strlen(buf));
                }
                break;
            }

            case SQL_BLOB:
            {
                isc_blob_handle blob_handle = NULL;
                int blob_stat;
                char blob_info_buffer[32], *p,
                    blob_segment_buffer[MAX_BLOB_SEGMENT];
                char blob_info_items[] =
                {
                    isc_info_blob_max_segment,
                    isc_info_blob_total_length
                };
                long max_segment = -1L, total_length = -1L, t;
                unsigned short seg_length;

            /*
             * Open the Blob according to the Blob id.
             * Here's where my internal st, tr, and db
             * data structures are really starting to pay off.
             */
                isc_open_blob2(
                    status,
                    &(imp_dbh->db),
                    &(imp_dbh->tr),
                    &blob_handle,
                    (ISC_QUAD *) var->sqldata,
                    (short) 0,      /* no Blob filter */
                    (char *) NULL   /* no Blob filter */
                );

                if (ib_error_check(sth, status))
                    return FALSE;

            /*
             * To find out the segment size in the proper way,
             * I must query the blob information.
             */

                isc_blob_info(
                    status,
                    &blob_handle,
                    sizeof(blob_info_items),
                    blob_info_items,
                    sizeof(blob_info_buffer),
                    blob_info_buffer
                );

                if (ib_error_check(sth, status))
                    return FALSE;

            /*
             * Get the information out of the info buffer.
             */
                for (p = blob_info_buffer; *p != isc_info_end; )
                {
                    short length;
                    char datum = *p++;

                    length = (short) isc_vax_integer(p, 2);
                    p += 2;
                    switch (datum)
                    {
                    case isc_info_blob_max_segment:
                        max_segment = isc_vax_integer(p, length);
                        break;
                    case isc_info_blob_total_length:
                        total_length = isc_vax_integer(p, length);
                        break;
                    }
                    p += length;
                }

                if (max_segment == -1L || total_length == -1L)
                {
                    isc_cancel_blob(status, &blob_handle);
                    do_error(
                        sth, 
                        1, 
                        "Cannot determine Blob dimensions.");
                    return FALSE;
                    break;
                }

                if ((DBIc_LongReadLen(imp_sth) < total_length) && 
                    (! DBIc_is(imp_dbh, DBIcf_LongTruncOk)))
                {
                    isc_cancel_blob(status, &blob_handle);
                    do_error(sth, 1, "Not enough LongReadLen buffer.");
                    return FALSE;
                    break;
                }

                if (total_length >= MAX_SAFE_BLOB_LENGTH)
                {
                    do_error(
                        sth, 
                        1,
                        "Blob exceeds maximum length.");

                    sv_setpvn(sv, "** Blob exceeds maximum safe length **", 38);
                /*
                 * I deliberately don't set FAILURE based on this.
                 */
                    break;
                }

            /*
             * Create a zero-length string.
             */
                sv_setpv(sv, "");

                t = total_length;
                while (1)
                {
                    blob_stat = isc_get_segment(
                        status,
                        &blob_handle,
                        &seg_length,
                        (short) MAX_BLOB_SEGMENT,
                        blob_segment_buffer
                    );
                    if (status[1] != isc_segment)
                        if (ib_error_check(sth, status))
                            return FALSE;
                    if (status[1] == isc_segstr_eof)
                        break;
                
                    if (seg_length > DBIc_LongReadLen(imp_sth))
                         break;
 
    /*
     * As long as the fetch was successful, concatenate the segment we fetched
     * into the growing Perl scalar.  
     */
    /*
     * This is dangerous if the Blob is enormous.  But Perl is supposed
     * to be able to grow scalars indefinitely as far as resources allow,
     * so what the heck.  Besides, I limited the max length of a Blob earlier
     * to MAX_SAFE_BLOB_LENGTH.
     */

                    sv_catpvn(sv, blob_segment_buffer, seg_length);
                    t -= seg_length;

                    if (t <= 0) break;
                    if (blob_stat == 100) break;
                }

            /*
             * Clean up after ourselves.
             */
                isc_close_blob(status, &blob_handle);
                if (ib_error_check(sth, status)) 
                    return FALSE;
            
                break;
            }
            
            case SQL_ARRAY:
#ifdef ARRAY_SUPPORT
            !!! NOT IMPLEMENTED YET !!!
#else
                sv_setpvn(sv, "** array **", 11);
#endif
                break;

            default:
                sv_setpvn(sv, "** unknown **", 13);

        }

        /*
         * I use the column's alias name because in the absence
         * of an alias, it contains the column name anyway.
         * Only if the alias AND the column names are zero-length
         * do I want to use a generic "COLUMN%d" header.
         * This happens, for example, when the column is a
         * computed field and the query doesn't use an AS clause
         * to label the column.
         */
/*
            if (var->aliasname_length > 0)
            {
                sv_setpvn(sv, var->aliasname, var->aliasname_length));
            }
            else
            {
                char s[20];
                sprintf(s, "COLUMN%d", i);
                sv_setpvn(sv, s, strlen(s));
            }
*/
        }
    }
	imp_sth->fetched += 1;
    return av;
}

/*
: benarkah jika gagal tidak perlu menon-aktifkan imp_sth?
*/

int dbd_st_finish(SV *sth, imp_sth_t *imp_sth)
{
    D_imp_dbh_from_sth;
    ISC_STATUS  status[ISC_STATUS_LENGTH]; 
    
    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_st_finish\n");

    if (!DBIc_ACTIVE(imp_sth)) /* already finished */
        return TRUE;

    /* Close the cursor, not drop the statement! */
    isc_dsql_free_statement(status, (isc_stmt_handle *)&(imp_sth->stmt), DSQL_close);

    /* XXX Wanna try DSQL_drop? 
    isc_dsql_free_statement(status, (isc_stmt_handle *)&(imp_sth->stmt), DSQL_drop);
    */
    
    if (ib_error_check(sth, status))
        return FALSE;

    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, 
        "dbd_st_finish: isc_dsql_free_statement passed.\n");

    /* if AutoCommit on */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
        if (!ib_commit_transaction(sth, imp_dbh))
            return FALSE;
    }

    if (dbis->debug >= 3) 
       PerlIO_printf(DBILOGFP, 
       "dbd_st_finish: IMPSET_on imp_sth? %d.\n", DBIc_IMPSET_on(imp_sth));

    DBIc_ACTIVE_off(imp_sth);
    return TRUE;
}

/*
: benarkah jika gagal tidak perlu return FALSE? 
*/

void dbd_st_destroy(SV *sth, imp_sth_t *imp_sth)
{
    ISC_STATUS  status[ISC_STATUS_LENGTH];

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_st_destroy\n");

    if (imp_sth->cursor_name != NULL)
    {
        safefree(imp_sth->cursor_name);
        imp_sth->cursor_name = NULL;
    }

    if (imp_sth->in_sqlda)
    {
        int i;
        XSQLVAR *var = imp_sth->in_sqlda->sqlvar;
        for (i=0; i < imp_sth->in_sqlda->sqld; i++, var++)
        {
            if (var->sqldata)
            {
                safefree(var->sqldata);
                var->sqldata = NULL;
            }

            if (var->sqltype & 1)
                safefree(var->sqlind);
        }

        if (dbis->debug >= 3) 
            PerlIO_printf(DBILOGFP, "dbd_st_destroy: freeing in_sqlda..\n");

        safefree(imp_sth->in_sqlda);
        imp_sth->in_sqlda = NULL;
    }

    if (imp_sth->out_sqlda)
    {
        int i;
        XSQLVAR *var = imp_sth->out_sqlda->sqlvar;
        for (i = 0; i < imp_sth->out_sqlda->sqld; i++, var++)
        {
            if (var->sqldata) {
                safefree(var->sqldata);
                var->sqldata = NULL;
            }

            if (var->sqltype & 1)
                safefree(var->sqlind);
        }
        safefree(imp_sth->out_sqlda); 
        imp_sth->out_sqlda = NULL;
    }

    /* Drop the statement and free the resource */
    if (imp_sth->stmt) {
        isc_dsql_free_statement(status, &(imp_sth->stmt), DSQL_drop); 
        if (ib_error_check(sth, status)) {
            if (dbis->debug >= 3) 
                PerlIO_printf(DBILOGFP, 
                "dbd_st_destroy: isc_dsql_free_statement fails.\n");
        } else 
            if (dbis->debug >= 3) 
                PerlIO_printf(DBILOGFP, 
                "dbd_st_destroy: isc_dsql_free_statement succeeds.\n");
    }
    imp_sth->stmt = 0L;
        
    if (imp_sth->ib_dateformat) {
        safefree(imp_sth->ib_dateformat);
        imp_sth->ib_dateformat = NULL;
    }

    if (imp_sth->ib_timeformat) {
        safefree(imp_sth->ib_timeformat);
        imp_sth->ib_timeformat = NULL;
    }

    if (imp_sth->ib_timestampformat) {
        safefree(imp_sth->ib_timestampformat);
        imp_sth->ib_timestampformat = NULL;
    }

    DBIc_IMPSET_off(imp_sth);
}

SV* dbd_st_FETCH_attrib(SV *sth, imp_sth_t *imp_sth, SV *keysv)
{
    STRLEN  kl;
    char *  key = SvPV(keysv, kl);
    int i;
    SV *    result = NULL;
    /* Default to caching results for DBI dispatch quick_FETCH  */
    int cacheit = TRUE;

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_st_FETCH - %s\n", key);

    if (kl==13 && strEQ(key, "NUM_OF_PARAMS"))  /* handled by DBI */
        return Nullsv;  

    if (!imp_sth->done_desc && !dbd_describe(sth, imp_sth)) {
        STRLEN lna;

    /* we can't return Nullsv here because the xs code will */
    /* then just pass the attribute name to DBI for FETCH.  */

        croak("Describe failed during %s->FETCH(%s): %ld: %s",
            SvPV(sth,na), key, (long)SvIV(DBIc_ERR(imp_sth)),
            SvPV(DBIc_ERRSTR(imp_sth),lna)
        );
    }

    i = DBIc_NUM_FIELDS(imp_sth);

    if (kl==4 && strEQ(key, "TYPE")) 
    {
        AV *av = newAV();
        result = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
            av_store(av, i, 
            newSViv(ib2sql_type(imp_sth->out_sqlda->sqlvar[i].sqltype)));
    } 
    else if (kl==5 && strEQ(key, "SCALE")) 
    {
        AV *av = newAV();
        result = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
            av_store(av, i, newSViv(imp_sth->out_sqlda->sqlvar[i].sqlscale));

    } 
    else if (kl==9 && strEQ(key, "PRECISION")) 
    {
        AV *av = newAV();
        result = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
            av_store(av, i, newSViv(imp_sth->out_sqlda->sqlvar[i].sqllen));
    } 
    else if (kl==4 && strEQ(key, "NAME")) {
        AV *av = newAV();
        result = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
		{
			if (imp_sth->out_sqlda->sqlvar[i].aliasname_length > 0)
			{
			    av_store(av, i, 
			    	newSVpvn(imp_sth->out_sqlda->sqlvar[i].aliasname, 
			    	imp_sth->out_sqlda->sqlvar[i].aliasname_length));
			}
			else
			{
		    	char s[20];
		    	sprintf(s, "COLUMN%d", i);
		    	av_store(av, i, 
		    		newSVpvn(s, strlen(s)));
			}
/*
            av_store(av, i,
                newSVpvn(imp_sth->out_sqlda->sqlvar[i].sqlname, 
                imp_sth->out_sqlda->sqlvar[i].sqlname_length)
            );
*/
		}
    } 
    else if (kl==8 && strEQ(key, "NULLABLE")) 
    {
        AV *av = newAV();
        result = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
            av_store(av, i, boolSV(imp_sth->out_sqlda->sqlvar[i].sqltype & 1 != 0));
    } else if (kl==10 && strEQ(key, "CursorName")) {
        if (imp_sth->cursor_name == NULL)
            return Nullsv;
        else
            result = newSVpv(imp_sth->cursor_name, strlen(imp_sth->cursor_name));
    } else 
    {
        return Nullsv;
    }
    
    if (cacheit) 
    { /* cache for next time (via DBI quick_FETCH)  */
        SV **svp = hv_fetch((HV*)SvRV(sth), key, kl, 1);
        sv_free(*svp);
        *svp = result;
        (void)SvREFCNT_inc(result); /* so sv_2mortal won't free it  */
    }
    return sv_2mortal(result);
}

int dbd_st_STORE_attrib(SV *sth, imp_sth_t *imp_sth, SV *keysv, SV *valuesv) 
{
    STRLEN  kl;
    char *  key = SvPV(keysv, kl);
    int     on  = SvTRUE(valuesv);

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_st_STORE - %s\n", key);

    if ((kl == 13) && (strcmp(key, "ib_cursorname")==0))
    {
        if (DBIc_ACTIVE(imp_sth))
        {
            do_error(sth, 1, "Can't modify active statement cursor name.");
            return FALSE;
        }
        else
        {
            STRLEN  vl;
            char *  value = SvPV(valuesv, vl);

            if (imp_sth->cursor_name != NULL)
                safefree(imp_sth->cursor_name);

            imp_sth->cursor_name = (char *)safemalloc(vl+1);

            if (imp_sth->cursor_name != NULL)
                strcpy(imp_sth->cursor_name, value);
            else
                return FALSE;
        }
    }

    return FALSE;
}

int dbd_discon_all(SV *drh, imp_drh_t *imp_drh)
{
    dTHR;

    /* The disconnect_all concept is flawed and needs more work */
    if (!SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) 
    {
        sv_setiv(DBIc_ERR(imp_drh), (IV)1);
        sv_setpv(DBIc_ERRSTR(imp_drh),
            (char*)"disconnect_all not implemented");
        DBIh_EVENT2(drh, ERROR_event,
            DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
        return FALSE;
    }
    if (perl_destruct_level)
        perl_destruct_level = 0;
    return FALSE;
}

int ib_blob_write (SV *sth, imp_sth_t *imp_sth, XSQLVAR *var, SV *value) 
{
    D_imp_dbh_from_sth;
    isc_blob_handle     handle  = NULL;
    ISC_STATUS          status[20];
    long                total_length;
    STRLEN              len;
    char *              p;
    char                blob_segment[BLOB_SEGMENT];
    
    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "ib_blob_write\n");

    var->sqldata = (char *)safemalloc(sizeof(ISC_QUAD));

    isc_create_blob2(status, &(imp_dbh->db), &(imp_dbh->tr), &handle,
        (ISC_QUAD *)(var->sqldata), 0, NULL);

    if (ib_error_check(sth, status))
        return FALSE;

    total_length = SvCUR(value);

    p = SvPV(value, len);

    if (var->sqlsubtype == isc_bpb_type_stream) /* SUBTYPE TEXT */
    {
        while (total_length > 0)
        {
            int     i = 0;

            while ((i<BLOB_SEGMENT) && (total_length > 0))
            {
                blob_segment[i] = *p++;
                total_length --;

                i++;

                if (blob_segment[i] == '\n')
                    break;
            }

            isc_put_segment(status, &handle, i, blob_segment);
            if (ib_error_check(sth, status))
            {
                isc_cancel_blob(status, &handle);
                return FALSE;
            }
        }
    }
    else
    {
        while (total_length > 0)
        {
            int     i = 0;
 
            while ((i<BLOB_SEGMENT) && (total_length > 0))
            {
                blob_segment[i] = *p++;
                total_length --;
            }

            isc_put_segment(status, &handle, i, blob_segment);
            if (ib_error_check(sth, status))
            {
                isc_cancel_blob(status, &handle);
                return FALSE;
            }

        }
    }

    isc_close_blob(status, &handle);
    if (ib_error_check(sth, status))
        return FALSE;

    return TRUE;
}

/* fill in_sqlda with bind parameters */
static int ib_fill_isqlda(SV *sth, imp_sth_t *imp_sth, SV *param, SV *value, IV sql_type)
{
    D_imp_dbh_from_sth;
    STRLEN len;
    ISC_STATUS status[ISC_STATUS_LENGTH];
    XSQLVAR *ivar;
    int retval;
    int i = (int)SvIV(param) - 1;
    
    retval = TRUE;
    ivar = &(imp_sth->in_sqlda->sqlvar[i]);
    
    if (dbis->debug >= 2) {
        PerlIO_printf(DBILOGFP, 
        "enter ib_fill_isqlda. processing %d XSQLVAR\n", i);
        if (sql_type) 
            PerlIO_printf(DBILOGFP, "   Type %ld", (long)sql_type);
    }
    
    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, 
        "ib_fill_isqlda: XSQLDA len: %d\n", imp_sth->in_sqlda->sqln);

    /*
     * NULL indicator
     */

    ivar->sqlind = (short *) safemalloc(1);

    /* *(ivar->sqlind) = ivar->sqltype & 1 ? 0 : 1; */

    *(ivar->sqlind) = 0; /* default assume non-NULL */
    ivar->sqldata = (char *)NULL;
    
    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, 
        "ib_fill_isqlda: sqlind: %d\n", *(ivar->sqlind));

    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "ib_fill_isqlda\n");

    if (!SvOK(value)) /* user passed an undef */
    {
        if (ivar->sqltype & 1) /* Field is NULLable */
        {
            /*
            * The user has passed 'undef' for this scalar
            * parameter and we use this to indicate that
            * the parameter should have a NULL state.
            */
            *(ivar->sqlind) = -1; /* NULL */

            /*
            * Hence no need to fill in sqldata for this
            * sqlvar, because it's NULL anyway.
            * Skip to next loop iteration.
            *
            continue; 
            */
            goto end;
        } else {
            /*
            * User passed an undef to a field that is not nullable.
            */
            char err[80];
            sprintf(err, "You have not provided a value for non-nullable parameter #%d.", i);
            do_error(sth, 1, err);
            retval = FALSE;
            goto end;
        }
    }

    switch (ivar->sqltype & ~1)
    {
    case SQL_VARYING:

        if ( ivar->sqldata == (char *) NULL )
        {
            if ((ivar->sqldata = (char *)safemalloc(sizeof(char)*(ivar->sqllen + 1)
                + sizeof(short))) == NULL)
            {
                do_error(sth, 2, "Cannot allocate buffer for TEXT input parameter \n");
                retval = FALSE;
                goto end;
            }
        }
        len = SvCUR(value);
        /* The first word of VARCHAR sqldata is the length */
         *((short *) ivar->sqldata) = len;
        /* is the scalar longer than the database field? */
    
        if (len > (sizeof(char)*(ivar->sqllen+1)))
        {
            char err[80];
            sprintf(err, "You are trying to put %d characters into a %d character field", len, (sizeof(char)*(ivar->sqllen+1)));
            do_error(sth, 2, err);
            retval = FALSE;
            goto end;
        } else {
            memcpy(ivar->sqldata+sizeof(short), (char*)SvPV_nolen(value), len);
        }
        ivar->sqldata[len + sizeof(short)] = '\0';

        break;

    case SQL_TEXT:
        len = SvCUR(value);
        if ( ivar->sqldata == (char *) NULL )
        {
            if ((ivar->sqldata = (char *)
                safemalloc(sizeof(char) * (ivar->sqllen + 1))) == NULL)
            {
                do_error(sth, 2, "Cannot allocate buffer for TEXT input parameter \n");
                retval = FALSE;
                goto end;
            }
        }

        /* is the scalar longer than the database field? */
        if (len > (sizeof(char)*(ivar->sqllen+1)))
        {
        /*
         * error? or truncate?
         */
            char err[80];
            sprintf(err, "You are trying to put %d characters into a %d character field", len, (sizeof(char)*(ivar->sqllen+1)));
            do_error(sth, 2, err);
            retval = FALSE;
            goto end;
        } else {
            memset(ivar->sqldata, ' ', ivar->sqllen);
            memcpy(ivar->sqldata, SvPV_nolen(value), len);
            if (dbis->debug >= 3) 
                dump_str(ivar->sqldata, ivar->sqllen);
        }
/* XXX Is this necessary? */
/*      ivar->sqldata[len] = '\0'; */
        break;

    case SQL_SHORT:
    {
        int scale = (int) (pow(10.0, (double) -ivar->sqlscale));
        char format[64];
        short p, q, r;

        if ( (ivar->sqldata == (char *) NULL) &&
            ((ivar->sqldata = (char *) safemalloc(sizeof(short))) == NULL))
        {
            do_error(sth, 2, "Cannot allocate buffer for SHORT input parameter ..\n");
            retval = FALSE;
            goto end;
        }

        /* See case SQL_INT64 for commentary. */
        sprintf(format, "%%d.%%%dd%%1d", -ivar->sqlscale);
        p = q = r = (short) 0;
        sscanf((char*)SvPV_nolen(value), format, &p, &q, &r);
        if (r >= 5)
        {
            q++;
            p += q / scale;
            q %= scale;
        }

        *(short *) (ivar->sqldata) = (short) (p * scale + q);
        break;
    }

    case SQL_LONG:
    {
        int scale = (int) (pow(10.0, (double) -ivar->sqlscale));
        char format[64];
        long p, q, r;

        if ( (ivar->sqldata == (char *) NULL) &&
        ((ivar->sqldata = (char *) safemalloc(sizeof(long))) == NULL))
        {
            do_error(sth, 2, "Cannot allocate buffer for LONG input parameter ..\n");
            retval = FALSE;
            goto end;
        }

        /* See case SQL_INT64 for commentary. */
        sprintf(format, "%%ld.%%%dld%%1ld", -ivar->sqlscale);
        p = q = r = (long) 0;
        sscanf((char*)SvPV_nolen(value), format, &p, &q, &r);
        if (r >= 5)
        {
            q++;
            p += q / scale;
            q %= scale;
        }

        *(long *) (ivar->sqldata) = (long) (p * scale + q);
        break;
    }

#ifdef SQL_INT64
    case SQL_INT64:
    {
        int scale = (int) (pow(10.0, (double) -ivar->sqlscale));
        char format[64];
        ISC_INT64 p, q, r;

        if ( (ivar->sqldata == (char *) NULL) &&
        ((ivar->sqldata = (char *) safemalloc(sizeof(ISC_INT64))) == NULL))
        {
            do_error(sth, 2, "Cannot allocate buffer for LONG input parameter ..\n");
            retval = FALSE;
            goto end;
        }

        /*
         * Here I handle both whole and scaled numerics.
         * The trick is to avoid converting the Perl scalar
         * to an IEEE floating value, because this would
         * introduce exactness errors in the conversion from
         * base-10 to base-2.
         *
         * I create a pattern for scanf() to read the whole
         * number portion (p), then a number of digits for
         * the scale (q), then one more digit (r) for the
         * round-up threshold.
         *
         * Note that sprintf replaces %% with a single %.
         * See the man page for sscanf() for more details
         * about how it interprets %Ld, %1Ld, etc.
         */
        sprintf(format, "%%Ld.%%%dLd%%1Ld", -ivar->sqlscale);
        p = q = r = (ISC_INT64) 0;

        /*
         * sscanf won't put any value into q and r if there's
         * nothing to the right of the decimal place;
         * q and r will remain zero, which works out fine
         * for the math that follows.
         */
        sscanf((char*)SvPV_nolen(value), format, &p, &q, &r);

        if (r >= 5)         /* Round up iff r is 5 or greater */
        {
            q++;            /* round q up by one */
            p += q / scale;     /* round p up by one if q overflows */
            q %= scale;     /* modulus if q overflows */
        }
        *(ISC_INT64 *) (ivar->sqldata) = (ISC_INT64) (p * scale + q);
        break;
    }
#endif

    case SQL_FLOAT:
        if ( (ivar->sqldata == (char *) NULL) &&
        ((ivar->sqldata = (char *) safemalloc(sizeof(float))) == NULL))
        {
            fprintf(stderr, "Cannot allocate buffer for FLOAT input parameter #%d\n", i);
            retval = FALSE;
            goto end;
        }
        *(float *) (ivar->sqldata) = SvNV(value);
        break;

    case SQL_DOUBLE:
        if ( (ivar->sqldata == (char *) NULL) &&
        ((ivar->sqldata = (char *) safemalloc(sizeof(double))) == NULL))
        {
            do_error(sth, 2, "Cannot allocate buffer for DOUBLE input parameter ..\n");
            retval = FALSE;
            goto end;
        }
        *(double *) (ivar->sqldata) = SvNV(value);
        break;

#ifndef _ISC_TIMESTAMP_
    case SQL_DATE:
#else
    case SQL_TIMESTAMP: /* V6.0 macro */
    case SQL_TYPE_TIME: /* V6.0 macro */
    case SQL_TYPE_DATE: /* V6.0 macro */
#endif
        /*
         * Coerce the date literal into a CHAR string, so as
         * to allow InterBase's internal date-string parsing
         * to interpret the date.
         */
        if (SvPOK(value))
        {
            char *datestring = SvPV(value, len);

            ivar->sqltype = SQL_TEXT | (ivar->sqltype&1);

            ivar->sqllen = len;
            if ( ivar->sqldata == (char *) NULL)
            {
            /*
             * I should not allocate based on len, I should allocate
             * a fixed length based on the max date/time string.
             * For now let's just call it 100.  Okay, 101.
             */
                if ((ivar->sqldata = (char *) safemalloc(sizeof(char)*101)) == NULL)
                {
                    do_error(sth, 2, "Cannot allocate buffer for DATE input parameter ..\n");
                    retval = FALSE;
                    goto end;
                }
            }
            memcpy(ivar->sqldata, datestring, len);
            ivar->sqldata[len] = '\0';
        }
        else if (SvROK(value))
        {
            AV *localtime = (AV *) SvRV(value);

            printf("XXX UNFINISHED FEATURE: input date as localtime() style list, util.c, line ~464\n");

        /*
         * load the list into a struct tm
         * then call isc_encode_date()
         * and input the SQL_TIMESTAMP value
         */
        }
        break;

    case SQL_BLOB:
    {
        isc_blob_handle blob_handle = NULL;
        ISC_QUAD blob_id;
        int blob_stat;
        long total_length, t;
        char *p;

        if ((total_length = SvCUR(value)) >= MAX_SAFE_BLOB_LENGTH)
        {
            char err[80];
            sprintf(err, "You are trying to put %d characters into a Blob, the max is %d", total_length, MAX_SAFE_BLOB_LENGTH);
            do_error(sth, 2, err);
            retval = FALSE;
            goto end;
        }
        if ( (ivar->sqldata == (char *) NULL) &&
        ((ivar->sqldata = (char *) safemalloc(sizeof(ISC_QUAD))) == NULL))
        {
            do_error(sth, 2, "Cannot allocate buffer for Blob input parameter ..\n");
            retval = FALSE;
            goto end;
        }

        isc_create_blob(
            status,
            &(imp_dbh->db),
            &(imp_dbh->tr),
            &blob_handle,            /* set by this function */
            &blob_id                 /* set by this function */
        );

        if (ib_error_check(sth, status))
        {
            retval = FALSE;
            goto end;
        }   
        t = total_length;
        p = (char*)SvPV_nolen(value);
        
        while (1) /* loop on segments */
        {
            char blob_segment_buffer[MAX_BLOB_SEGMENT];
            char *q;

            q = blob_segment_buffer;
            if (ivar->sqlsubtype == isc_bpb_type_stream /* text */)
            {
            /*
             * Copy segment, up to and including newline,
             * or MAX_BLOB_SEGMENT bytes, whichever comes first.
             */
                while (q - blob_segment_buffer <= MAX_BLOB_SEGMENT && t > 0)
                {
                    --t;
                    if ((*q++ = *p++) == '\n') break;
                }
            } else  /* subtype not text */ {
                    /*
                     * Copy segment, up to MAX_BLOB_SEGMENT bytes.
                     */
                while (q - blob_segment_buffer <= MAX_BLOB_SEGMENT && t > 0)
                {
                    --t;
                    *q++ = *p++;
                }
            }
            blob_stat = isc_put_segment(
                status,
                &blob_handle,
                (unsigned short) (q - blob_segment_buffer),
                blob_segment_buffer
            );
            if (ib_error_check(sth, status))
            {
                retval = FALSE;
                goto end;
            }   

            if (t <= 0) break;
        }

        /*
         * Clean up after ourselves.
         */
        isc_close_blob(status, &blob_handle);
        if (ib_error_check(sth, status))
        {
            retval = FALSE;
            goto end;
        }   

        memcpy((ISC_QUAD *) ivar->sqldata, &blob_id, sizeof(ISC_QUAD));
        break;
    }
    /*    break; */

    case SQL_ARRAY:
#ifdef ARRAY_SUPPORT
        !!! NOT IMPLEMENTED YET !!!
#endif
        break;

    default:
        break;
    }

    end:
    return retval;
}

int dbd_bind_ph(SV *sth, imp_sth_t *imp_sth, SV *param, SV *value,
    IV sql_type, SV *attribs, int is_inout, IV maxlen)
{
    STRLEN      len;
    XSQLVAR *   var;

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "dbd_bind_ph\n");

    if (SvTYPE(value) > SVt_PVLV)
        croak("Can't bind a non-scalar value (%s)", neatsvpv(value,0));
    
/* is_inout for stored procedure is not implemented yet */
    if (is_inout)
        croak("Can't bind ``lvalue'' mode.");
    
/* param is the number of parameter: 1, 2, 3, or ... */
    if ((int)SvIV(param) > imp_sth->in_sqlda->sqld)
        return TRUE;
    
    var = &(imp_sth->in_sqlda->sqlvar[(int)SvIV(param)-1]);
    
    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "Binding parameter: %d\n", (int)SvIV(param));
    
    return ib_fill_isqlda(sth, imp_sth, param, value, sql_type);
}

/* start a transaction with tpb TPB, len length */
int ib_start_transaction(SV *h, imp_dbh_t *imp_dbh, char *tpb, unsigned short len)
{
    ISC_STATUS status[ISC_STATUS_LENGTH];

    if (imp_dbh->tr) {
        if (dbis->debug >= 3) 
            PerlIO_printf(DBILOGFP, 
            "ib_start_transaction: trans handle already started.\n");
        return TRUE;
    }
    
    if (tpb == NULL) {
        /* setup a default TPB */
        if ((imp_dbh->tpb_buffer = (char*)safemalloc(10 * sizeof(char))) == NULL) {
            do_error(h, IB_ALLOC_FAIL, "Can't alloc TPB");
            return FALSE;
        }
        tpb = imp_dbh->tpb_buffer;
        *tpb++ = isc_tpb_version3;
        *tpb++ = isc_tpb_write;
        *tpb++ = isc_tpb_concurrency;
        *tpb++ = isc_tpb_wait;
        imp_dbh->tpb_length = tpb - imp_dbh->tpb_buffer;
    } else {
        /* TPB already allocated, as pointed by tpb */
        imp_dbh->tpb_buffer = tpb;
        imp_dbh->tpb_length = len;
    }

    /* MUST initialized to 0, before it is used */
    imp_dbh->tr = 0L;
    
    isc_start_transaction(status, &(imp_dbh->tr), 1, &(imp_dbh->db),
       imp_dbh->tpb_length, imp_dbh->tpb_buffer );

    if (ib_error_check(h, status))
        return FALSE;

    if (dbis->debug >= 3) 
        PerlIO_printf(DBILOGFP, "ib_start_transaction: transaction started.\n");

    return TRUE;
}

int ib_commit_transaction(SV *h, imp_dbh_t *imp_dbh)
{
    ISC_STATUS status[ISC_STATUS_LENGTH];
    if (!imp_dbh->tr) {
        if (dbis->debug >= 3) 
            PerlIO_printf(DBILOGFP, 
            "ib_commit_transaction: transaction already NULL.\n");
        return TRUE;
    }   

    isc_commit_transaction(status, &(imp_dbh->tr));
    if (ib_error_check(h, status)) 
        return FALSE;

    if (dbis->debug >= 2) 
        PerlIO_printf(DBILOGFP, "ib_commit_transaction succeed.\n");

    imp_dbh->tr = 0L;
    return TRUE;
}

int dbd_st_blob_read(SV *sth, imp_sth_t *imp_sth, int field,
    long offset, long len, SV *destrv, long destoffset)
{
    return FALSE;
}

int dbd_st_rows(SV* sth, imp_sth_t* imp_sth) {
    /* spot common mistake of checking $h->rows just after ->execut
    if (imp_sth->fetched < 0 
    && DBIc_WARN(imp_sth)	
    ) {
	warn("$h->rows count is incomplete before all rows fetched.\n");
    }
*/
	if (imp_sth->type == isc_info_sql_stmt_select)  
	    return imp_sth->fetched;
	else 
		return -1; /* unknown */
}

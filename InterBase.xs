/*
   $Id: InterBase.xs,v 1.43 2003/12/06 11:04:10 edpratomo Exp $

   Copyright (c) 1999-2003  Edwin Pratomo
   Portions Copyright (c) 2001-2003  Daniel Ritz

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.

*/

#include "InterBase.h"

/* callback function for events, called by InterBase */
isc_callback _async_callback(IB_EVENT ISC_FAR *ev, short length, char ISC_FAR *updated)
{
#if defined(USE_THREADS) || defined(USE_ITHREADS) || defined(MULTIPLICITY)
    /* save context, set context from dbh */
    void *context = PERL_GET_CONTEXT;
    PERL_SET_CONTEXT(ev->dbh->context);
    {
#else
    void *context = PERL_GET_CONTEXT;
    PerlInterpreter *cb_perl = perl_alloc();
    PERL_SET_CONTEXT(cb_perl);
    {
#endif
        dSP;
        char ISC_FAR *result = ev->result_buffer;

        ENTER;
        SAVETMPS;

        PUSHMARK(SP);
        PUTBACK;

        perl_call_sv(ev->perl_cb, G_VOID);

        FREETMPS;
        LEAVE;

        /* Copy the updated buffer to the result buffer */
        while (length--)
            *result++ = *updated++;

        ev->cb_called = 1;
#if defined(USE_THREADS) || defined(USE_ITHREADS) || defined(MULTIPLICITY)
    }

    /* restore old context*/
    PERL_SET_CONTEXT(context);
#else
    }
    PERL_SET_CONTEXT(context);
    perl_free(cb_perl);
#endif

    return (0);
}

DBISTATE_DECLARE;

MODULE = DBD::InterBase     PACKAGE = DBD::InterBase

INCLUDE: InterBase.xsi

MODULE = DBD::InterBase     PACKAGE = DBD::InterBase::db

void
_do(dbh, statement, attr=Nullsv)
    SV *        dbh
    SV *    statement
    SV *        attr
  PROTOTYPE: $$;$@
  CODE:
{
    D_imp_dbh(dbh);
    ISC_STATUS status[ISC_STATUS_LENGTH]; /* isc api status vector    */
    STRLEN     slen;
    int        retval;
    char       *sbuf = SvPV(statement, slen);

    DBI_TRACE(1, (DBILOGFP, "db::_do\n"
                            "Executing : %s\n", sbuf));

    /* we need an open transaction */
    if (!imp_dbh->tr)
    {
        DBI_TRACE(1, (DBILOGFP, "starting new transaction..\n"));

        if (!ib_start_transaction(dbh, imp_dbh))
        {
            retval = -2;
            XST_mUNDEF(0);      /* <= -2 means error        */
            return;
        }

        DBI_TRACE(1, (DBILOGFP, "new transaction started.\n"));
    }

    /* only execute_immediate statment if NOT in soft commit mode */
    if (!(imp_dbh->soft_commit))
    {
        isc_dsql_execute_immediate(status, &(imp_dbh->db), &(imp_dbh->tr), 0,
                                   sbuf, imp_dbh->sqldialect, NULL);

        if (ib_error_check(dbh, status))
            retval = -2;
        else
            retval = -1 ;
    }
    /* for AutoCommit: prepare/getinfo/exec statment (count DDL statements)
     * an easier and also working way would be to do that from perl with
     *   $sth = $dbh->prepare(...); $sth->execute();
     * but this way is much faster (no bind params, etc.)
     */
    else
    {
        isc_stmt_handle stmt = 0L;        /* temp statment handle */
        static char     stmt_info[] = { isc_info_sql_stmt_type };
        char            info_buffer[20];  /* statment info buffer */

        retval = -2;

        do
        {
            /* init statement handle */
            if (isc_dsql_alloc_statement2(status, &(imp_dbh->db), &stmt))
                break;


            /* prepare statement */
            isc_dsql_prepare(status, &(imp_dbh->tr), &stmt, 0, sbuf,
                             imp_dbh->sqldialect, NULL);
            if (ib_error_check(dbh, status))
                break;


            /* get statement type */
            if (!isc_dsql_sql_info(status, &stmt, sizeof(stmt_info), stmt_info,
                              sizeof(info_buffer), info_buffer))
            {
                /* need to count DDL statments */
                short l = (short) isc_vax_integer((char *) info_buffer + 1, 2);
                if (isc_vax_integer((char *) info_buffer + 3, l) == isc_info_sql_stmt_ddl)
                    imp_dbh->sth_ddl++;
            }
            else
                break;


            /* exec the statement */
            isc_dsql_execute(status, &(imp_dbh->tr), &stmt, imp_dbh->sqldialect, NULL);
            if (!ib_error_check(dbh, status))
                retval = -1;

        } while (0);

        /* close statement */
        if (stmt)
           isc_dsql_free_statement(status, &stmt, DSQL_drop);

        if (retval != -2) retval = -1;
    }

    /* for AutoCommit: commit */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
    {
        if (!ib_commit_transaction(dbh, imp_dbh))
            retval = -2;
    }

    if (retval < -1)
        XST_mUNDEF(0);
    else
        XST_mIV(0, retval); /* typically 1, rowcount or -1  */
}

int
_ping(dbh)
    SV *    dbh
    CODE:
{
    int ret;
    ret = dbd_db_ping(dbh);
    if (ret == 0)
        XST_mUNDEF(0);
    else
        XST_mIV(0, ret);
}

void
ib_set_tx_param(dbh, ...)
    SV *dbh
    ALIAS:
    set_tx_param = 1
    PREINIT:
    STRLEN len;
    char   *tx_key, *tx_val, *tpb, *tmp_tpb;
    int    i, rc = 0;
    int    tpb_len;
    char   am_set = 0, il_set = 0, ls_set = 0;
    I32    j;
    AV     *av;
    HV     *hv;
    SV     *sv, *sv_value;
    HE     *he;

    CODE:
{
    D_imp_dbh(dbh);

    /* if no params or first parameter = 0 or undef -> reset TPB to NULL */
    if (items < 3)
    {
        if ((items == 1) || !(SvTRUE(ST(1))))
        {
            tpb     = NULL;
            tmp_tpb = NULL;
            tpb_len = 0;
            goto do_set_tpb;
        }
    }

    /* we need to know the max. size of TBP, (buffer overflow problem) */
    /* mem usage: -access_mode:     max. 1 byte                        */
    /*            -isolation_level: max. 2 bytes                       */
    /*            -lock_resolution: max. 1 byte                        */
    /*            -reserving:       max. 4 bytes + strlen(tablename)   */
    tpb_len = 5; /* 4 + 1 for tpb_version                              */

    /* we need to add the length of each table name + 4 bytes */
    for (i = 1; i < items-1; i += 2)
    {
        sv_value = ST(i + 1);
        if (strEQ(SvPV_nolen(ST(i)), "-reserving"))
            if (SvROK(sv_value) && SvTYPE(SvRV(sv_value)) == SVt_PVHV)
            {
                hv = (HV *)SvRV(sv_value);
                hv_iterinit(hv);
                while (he = hv_iternext(hv))
                {
                    /* retrieve the size of table name(s) */
                    HePV(he, len);
                    tpb_len += len + 4;
                }
            }
    }

    /* alloc it */
    tmp_tpb = (char *)safemalloc(tpb_len * sizeof(char));

    if (tmp_tpb == NULL)
        croak("set_tx_param: Can't alloc memory");

    /* do set TPB values */
    tpb = tmp_tpb;
    *tpb++ = isc_tpb_version3;

    for (i = 1; i < items; i += 2)
    {
        tx_key   = SvPV_nolen(ST(i));
        sv_value = ST(i + 1);

        /* value specified? */
        if (i >= items - 1)
        {
            safefree(tmp_tpb);
            croak("You must specify parameter => value pairs, but theres no value for %s", tx_key);
        }

        /**********************************************************************/
        if (strEQ(tx_key, "-access_mode"))
        {
            if (am_set)
            {
                warn("-access_mode already set; ignoring second try!");
                continue;
            }

            tx_val = SvPV_nolen(sv_value);
            if (strEQ(tx_val, "read_write"))
                *tpb++ = isc_tpb_write;
            else if (strEQ(tx_val, "read_only"))
                *tpb++ = isc_tpb_read;
            else
            {
                safefree(tmp_tpb);
                croak("Unknown -access_mode value %s", tx_val);
            }

            am_set = 1; /* flag */
        }
        /**********************************************************************/
        else if (strEQ(tx_key, "-isolation_level"))
        {
            if (il_set)
            {
                warn("-isolation_level already set; ignoring second try!");
                continue;
            }

            if (SvROK(sv_value) && SvTYPE(SvRV(sv_value)) == SVt_PVAV)
            {
                av = (AV *)SvRV(sv_value);

                /* sanity check */
                for (j = 0; (j <= av_len(av)) && !rc; j++)
                {
                    sv = *av_fetch(av, j, FALSE);
                    if (strEQ(SvPV_nolen(sv), "read_committed"))
                    {
                        rc = 1;
                        *tpb++ = isc_tpb_read_committed;
                    }
                }

                if (!rc)
                {
                    safefree(tmp_tpb);
                    croak("Invalid -isolation_level value");
                }

                for (j = 0; j <= av_len(av); j++)
                {
                    tx_val = SvPV_nolen(*(av_fetch(av, j, FALSE)));
                    if (strEQ(tx_val, "record_version"))
                    {
                        *tpb++ = isc_tpb_rec_version;
                        break;
                    }
                    else if (strEQ(tx_val, "no_record_version"))
                    {
                        *tpb++ = isc_tpb_no_rec_version;
                        break;
                    }
                    else if (!strEQ(tx_val, "read_committed"))
                    {
                        safefree(tmp_tpb);
                        croak("Unknown -isolation_level value %s", tx_val);
                    }
                }
            }
            else
            {
                tx_val = SvPV_nolen(sv_value);
                if (strEQ(tx_val, "read_committed"))
                    *tpb++ = isc_tpb_read_committed;
                else if (strEQ(tx_val, "snapshot"))
                    *tpb++ = isc_tpb_concurrency;
                else if (strEQ(tx_val, "snapshot_table_stability"))
                    *tpb++ = isc_tpb_consistency;
                else
                {
                    safefree(tmp_tpb);
                    croak("Unknown -isolation_level value %s", tx_val);
                }
            }

            il_set = 1; /* flag */
        }
        /**********************************************************************/
        else if (strEQ(tx_key, "-lock_resolution"))
        {
            if (ls_set)
            {
                warn("-lock_resolution already set; ignoring second try!");
                continue;
            }

            tx_val = SvPV_nolen(sv_value);
            if (strEQ(tx_val, "wait"))
                *tpb++ = isc_tpb_wait;
            else if (strEQ(tx_val, "no_wait"))
                *tpb++ = isc_tpb_nowait;
            else
            {
                safefree(tmp_tpb);
                croak("Unknown transaction parameter %s", tx_val);
            }

            ls_set = 1; /* flag */
        }
        /**********************************************************************/
        else if (strEQ(tx_key, "-reserving"))
        {
            if (SvROK(sv_value) && SvTYPE(SvRV(sv_value)) == SVt_PVHV)
            {
                char *table_name;
                HV *table_opts;
                hv = (HV *)SvRV(sv_value);
                hv_iterinit(hv);
                while (he = hv_iternext(hv))
                {
                    /* check val type */
                    if (SvROK(HeVAL(he)) && SvTYPE(SvRV(HeVAL(he))) == SVt_PVHV)
                    {
                        table_opts = (HV*)SvRV(HeVAL(he));

                        if (hv_exists(table_opts, "access", 6))
                        {
                            /* access is optional */
                            sv = *hv_fetch(table_opts, "access", 6, FALSE);
                            if (strnEQ(SvPV_nolen(sv), "shared", 6))
                                *tpb++ = isc_tpb_shared;
                            else if (strnEQ(SvPV_nolen(sv), "protected", 9))
                                *tpb++ = isc_tpb_protected;
                            else
                            {
                                safefree(tmp_tpb);
                                croak("Invalid -reserving access value");
                            }
                        }

                        if (hv_exists(table_opts, "lock", 4))
                        {
                            /* lock is required */
                            sv = *hv_fetch(table_opts, "lock", 4, FALSE);
                            if (strnEQ(SvPV_nolen(sv), "read", 4))
                               *tpb++ = isc_tpb_lock_read;
                            else if (strnEQ(SvPV_nolen(sv), "write", 5))
                               *tpb++ = isc_tpb_lock_write;
                            else
                            {
                              safefree(tmp_tpb);
                              croak("Invalid -reserving lock value");
                            }
                        }
                        else /* lock */
                        {
                            safefree(tmp_tpb);
                            croak("Lock value is required in -reserving");
                        }

                        /* add the table name to TPB */
                        table_name = HePV(he, len);
                        *tpb++ = len + 1;
                        {
                            unsigned int k;
                            for (k = 0; k < len; k++)
                                *tpb++ = toupper(*table_name++);
                        }
                        *tpb++ = 0;
                    } /* end hashref check*/
                    else
                    {
                        safefree(tmp_tpb);
                        croak("Reservation for a given table must be hashref.");
                    }
                } /* end of while() */
            }
            else
            {
                safefree(tmp_tpb);
                croak("Invalid -reserving value. Must be hashref.");
            }
        } /* end table reservation */
        else
        {
            safefree(tmp_tpb);
            croak("Unknown transaction parameter %s", tx_key);
        }
    }

    /* an ugly label... */
    do_set_tpb:

    safefree(imp_dbh->tpb_buffer);
    imp_dbh->tpb_buffer = tmp_tpb;
    imp_dbh->tpb_length = tpb - imp_dbh->tpb_buffer;

    /* for AutoCommit: commit current transaction */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
    {
        imp_dbh->sth_ddl++;
        ib_commit_transaction(dbh, imp_dbh);
    }
}
#*******************************************************************************

# only for use within database_info!
#define DB_INFOBUF(name, len) \
if (strEQ(item, #name)) { \
    *p++ = (char) isc_info_##name; \
    res_len += len + 3; \
    item_buf_len++; \
    continue; \
}

#define DB_RESBUF_CASEHDR(name) \
case isc_info_##name:\
    keyname = #name;


HV *
ib_database_info(dbh, ...)
    SV *dbh
    PREINIT:
    unsigned int i, count;
    char  item_buf[30], *p, *old_p;
    char *res_buf;
    short item_buf_len, res_len;
    AV    *av;
    ISC_STATUS status[ISC_STATUS_LENGTH];
    CODE:
{
    D_imp_dbh(dbh);

    /* process input params, count max. result buffer length */
    p = item_buf;
    res_len = 0;
    item_buf_len = 0;

    /* array or array ref? */
    if (items == 2 && SvROK(ST(1)) && SvTYPE(SvRV(ST(1))) == SVt_PVAV)
    {
        av    = (AV *)SvRV(ST(1));
        count = av_len(av) + 1;
    }
    else
    {
        av    = NULL;
        count = items;
    }

    /* loop thru all elements */
    for (i = 0; i < count; i++)
    {
        char *item;

        /* fetch from array or array ref? */
        if (av)
            item = SvPV_nolen(*av_fetch(av, i, FALSE));
        else
            item = SvPV_nolen(ST(i + 1));

        /* database characteristics */
        DB_INFOBUF(allocation,        4);
        DB_INFOBUF(base_level,        2);
        DB_INFOBUF(db_id,           513);
        DB_INFOBUF(implementation,    3);
        DB_INFOBUF(no_reserve,        1);
#ifdef IB_API_V6
        DB_INFOBUF(db_read_only,      1);
#endif
        DB_INFOBUF(ods_minor_version, 1);
        DB_INFOBUF(ods_version,       1);
        DB_INFOBUF(page_size,         4);
        DB_INFOBUF(version,         257);
#ifdef IB_API_V6
        DB_INFOBUF(db_sql_dialect,    1);
#endif

        /* environmental characteristics */
        DB_INFOBUF(current_memory,    4);
        DB_INFOBUF(forced_writes,     1);
        DB_INFOBUF(max_memory,        4);
        DB_INFOBUF(num_buffers,       4);
        DB_INFOBUF(sweep_interval,    4);
        DB_INFOBUF(user_names,     1024); /* can be more, can be less */

        /* performance statistics */
        DB_INFOBUF(fetches, 4);
        DB_INFOBUF(marks,   4);
        DB_INFOBUF(reads,   4);
        DB_INFOBUF(writes,  4);

        /* database operation counts */
        /* XXX - not implemented (complicated: returns a descriptor for _each_
           table...how to fetch / store this??) but do we really need these? */
    }

    /* the end marker */
    *p++ = isc_info_end;
    item_buf_len++;

    /* allocate the result buffer */
    res_len += 256; /* add some safety...just in case */
    res_buf = (char *) safemalloc(res_len);

    /* call the function */
    isc_database_info(status, &(imp_dbh->db), item_buf_len, item_buf,
                      res_len, res_buf);

    if (ib_error_check(dbh, status))
    {
        safefree(res_buf);
        croak("isc_database_info failed!");
    }

    /* create a hash if function passed */
    RETVAL = newHV();
    if (!RETVAL)
    {
        safefree(res_buf);
        croak("unable to allocate hash return value");
    }

    /* fill the hash with key/value pairs */
    for (p = res_buf; *p != isc_info_end; )
    {
        char *keyname;
        char item   = *p++;
        int  length = isc_vax_integer (p, 2);
        p += 2;
        old_p = p;

        switch (item)
        {
            /******************************************************************/
            /* database characteristics */
            DB_RESBUF_CASEHDR(allocation)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(base_level)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(++p, 1)), 0);
                break;

            DB_RESBUF_CASEHDR(db_id)
            {
                HV *reshv = newHV();
                long slen;

                hv_store(reshv, "connection", 10,
                         (isc_vax_integer(p++, 1) == 2)?
                             newSVpv("local", 0):
                             newSVpv("remote", 0),
                         0);

                slen = isc_vax_integer(p++, 1);
                hv_store(reshv, "database", 8, newSVpvn(p, slen), 0);
                p += slen;

                slen = isc_vax_integer(p++, 1);
                hv_store(reshv, "site", 8, newSVpvn(p, slen), 0);

                hv_store(RETVAL, keyname, strlen(keyname),
                         newRV_noinc((SV *) reshv), 0);
                break;
            }

            DB_RESBUF_CASEHDR(implementation)
            {
                HV *reshv = newHV();

                hv_store(reshv, "implementation", 14,
                         newSViv(isc_vax_integer(++p, 1)), 0);

                hv_store(reshv, "class", 5,
                         newSViv(isc_vax_integer(++p, 1)), 0);

                hv_store(RETVAL, keyname, strlen(keyname),
                         newRV_noinc((SV *) reshv), 0);

                break;
            }

            DB_RESBUF_CASEHDR(no_reserve)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;
#ifdef IB_API_V6
            DB_RESBUF_CASEHDR(db_read_only)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;
#endif
            DB_RESBUF_CASEHDR(ods_minor_version)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(ods_version)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(page_size)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(version)
            {
                long slen;
                slen = isc_vax_integer(++p, 1);
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSVpvn(++p, slen), 0);
                break;
            }
#ifdef isc_dpb_sql_dialect
            DB_RESBUF_CASEHDR(db_sql_dialect)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;
#endif

            /******************************************************************/
            /* environmental characteristics */
            DB_RESBUF_CASEHDR(current_memory)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(forced_writes)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(max_memory)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(num_buffers)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(sweep_interval)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(user_names)
            {
                AV *avres;
                SV **svp;
                long slen;

                /* array already existing? no -> create */
                if (!hv_exists(RETVAL, "user_names", 10))
                {
                    avres = newAV();
                    hv_store(RETVAL, "user_names", 10,
                             newRV_noinc((SV *) avres), 0);
                }
                else
                {
                    svp = hv_fetch(RETVAL, "user_names", 10, 0);
                    if (!svp || !SvROK(*svp))
                    {
                        safefree(res_buf);
                        croak("Error fetching hash value");
                    }

                    avres = (AV *) SvRV(*svp);
                }

                /* add value to the array */
                slen = isc_vax_integer(p++, 1);
                av_push(avres, newSVpvn(p, slen));

                break;
            }

            /******************************************************************/
            /* performance statistics */
            DB_RESBUF_CASEHDR(fetches)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(marks)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(reads)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            DB_RESBUF_CASEHDR(writes)
                hv_store(RETVAL, keyname, strlen(keyname),
                         newSViv(isc_vax_integer(p, (short) length)), 0);
                break;

            default:
                break;
        }

        p = old_p + length;
    }

    /* don't leak */
    safefree(res_buf);
}
    OUTPUT:
    RETVAL

    CLEANUP:
    SvREFCNT_dec(RETVAL);

#undef DB_INFOBUF
#undef DB_RESBUF_CASEHDR

#*******************************************************************************

IB_EVENT *
ib_init_event(dbh, ...)
    SV *dbh
    PREINIT:
    char *CLASS = "DBD::InterBase::Event";
    int i;
    D_imp_dbh(dbh);
    CODE:
{
    unsigned short cnt = items - 1;

    DBI_TRACE(2, (DBILOGFP, "Entering init_event(), %d items..\n", cnt));

    if (cnt > 0)
    {
        /* check for max number of events in a single call to event block allocation */
        if (cnt > MAX_EVENTS)
            croak("Max number of events exceeded.");

        RETVAL = (IB_EVENT *) safemalloc(sizeof(IB_EVENT));
        if (RETVAL == NULL)
            croak("Unable to allocate memory");

        /* init members */
        RETVAL->dbh           = imp_dbh;
        RETVAL->event_buffer  = NULL;
        RETVAL->result_buffer = NULL;
        RETVAL->id            = 0;
        RETVAL->reinit        = 0;
        RETVAL->num           = cnt;
        RETVAL->perl_cb       = NULL;
        RETVAL->cb_called     = 0;

        RETVAL->names = (char **) safemalloc(sizeof(char*) * MAX_EVENTS);
        if (RETVAL->names == NULL)
            croak("Unable to allocate memory");

        for (i = 0; i < MAX_EVENTS; i++)
        {
            if (i < cnt) {
                /* dangerous! 
                *(RETVAL->names + i) = SvPV_nolen(ST(i + 1));
                */
                RETVAL->names[i] = (char*) safemalloc(sizeof(char) * (SvCUR(ST(i + 1)) + 1));
                if (RETVAL->names[i] == NULL) 
                    croak("Unable to allocate memory");
                strcpy(RETVAL->names[i], SvPV_nolen(ST(i + 1)));
            }
            else
                *(RETVAL->names + i) = NULL;
        }

        RETVAL->epb_length = (short) isc_event_block(
            &(RETVAL->event_buffer),
            &(RETVAL->result_buffer),
            cnt,
            *(RETVAL->names +  0),
            *(RETVAL->names +  1),
            *(RETVAL->names +  2),
            *(RETVAL->names +  3),
            *(RETVAL->names +  4),
            *(RETVAL->names +  5),
            *(RETVAL->names +  6),
            *(RETVAL->names +  7),
            *(RETVAL->names +  8),
            *(RETVAL->names +  9),
            *(RETVAL->names + 10),
            *(RETVAL->names + 11),
            *(RETVAL->names + 12),
            *(RETVAL->names + 13),
            *(RETVAL->names + 14));
    }
    else
        croak("Names of the events in interest are not specified");

    DBI_TRACE(2, (DBILOGFP, "Leaving init_event()\n"));
}
    OUTPUT:
    RETVAL


int
ib_register_callback(dbh, ev, perl_cb)
    SV *dbh
    IB_EVENT *ev
    SV *perl_cb
    PREINIT:
    ISC_STATUS status[ISC_STATUS_LENGTH];
    D_imp_dbh(dbh);
    CODE:
{
    DBI_TRACE(2, (DBILOGFP, "Entering register_callback()..\n"));

    /* save the perl callback function */
    ev->perl_cb = perl_cb;

    /* set up the events */
    isc_que_events(
        status,
        &(imp_dbh->db),
        &(ev->id),
        ev->epb_length,
        ev->event_buffer,
        (isc_callback)_async_callback,
        ev);

    if (ib_error_check(dbh, status))
        RETVAL = 0;
    else
        RETVAL = 1;

    DBI_TRACE(2, (DBILOGFP, "Leaving register_callback(): %d\n", RETVAL));
}
    OUTPUT:
    RETVAL


HV*
ib_reinit_event(dbh, ev)
    SV *dbh;
    IB_EVENT *ev
    PREINIT:
    int i;
    SV  **svp;
    ISC_STATUS status[ISC_STATUS_LENGTH];
    CODE:
{
    DBI_TRACE(2, (DBILOGFP, "reinit_event() - reinit value: %d.\n",
                  (int)ev->reinit));

    RETVAL = newHV();
    /* if (ev->reinit) { */
    if (1)
    {
        isc_event_counts(status, ev->epb_length, ev->event_buffer,
                         ev->result_buffer);
        for (i = 0; i < ev->num; i++)
        {
            if (status[i])
            {
                DBI_TRACE(2, (DBILOGFP, "Event %s caught %ld times.\n",
                              *(ev->names + i), status[i]));

                svp = hv_store(RETVAL, *(ev->names + i), strlen(*(ev->names + i)),
                               newSViv(status[i]), 0);
                if (svp == NULL)
                    croak("Bad: key '%s' not stored", *(ev->names + i));
            }
        }
    }
    else
        ev->reinit = 1;

    ev->cb_called = 0;
}
    OUTPUT:
    RETVAL
    CLEANUP:
    SvREFCNT_dec(RETVAL);


int
ib_cancel_callback(dbh, ev)
    SV *dbh
    IB_EVENT *ev
    PREINIT:
    ISC_STATUS status[ISC_STATUS_LENGTH];
    D_imp_dbh(dbh);
    CODE:
{
    DBI_TRACE(2, (DBILOGFP, "Entering cancel_callback()..\n"));

    if (ev->perl_cb)
        isc_cancel_events(status, &(imp_dbh->db), &(ev->id));

    if (ib_error_check(dbh, status))
    {
        do_error(dbh, 2, "cancel_callback() error");
        RETVAL = 0;
    }
    else
        RETVAL = 1;
}
    OUTPUT:
    RETVAL


int
ib_wait_event(dbh, ev)
    SV *dbh
    IB_EVENT *ev
    PREINIT:
    ISC_STATUS status[ISC_STATUS_LENGTH];
    D_imp_dbh(dbh);
    CODE:
{
    isc_wait_for_event(status, &(imp_dbh->db), ev->epb_length, ev->event_buffer,
                       ev->result_buffer);

    if (ib_error_check(dbh, status))
    {
        do_error(dbh, 2, "wait_event() error");
        RETVAL = 0;
    }
    else
        RETVAL = 1;
}
    OUTPUT:
    RETVAL


MODULE = DBD::InterBase     PACKAGE = DBD::InterBase::Event
PROTOTYPES: DISABLE

void
DESTROY(evh)
    IB_EVENT *evh
    PREINIT:
    int i;
    ISC_STATUS status[ISC_STATUS_LENGTH];
    CODE:
{
    DBI_TRACE(2, (DBILOGFP, "Entering DBD::InterBase::Event destructor..\n"));

    for (i = 0; i < evh->num; i++)
        if (*(evh->names + i))
            safefree(*(evh->names + i));
    if (evh->names)
        safefree(evh->names);
    if (evh->perl_cb)
        isc_cancel_events(status, &(evh->dbh->db), &(evh->id));
    if (evh->event_buffer)
        isc_free(evh->event_buffer);
    if (evh->result_buffer)
        isc_free(evh->result_buffer);
}

int
callback_called(evh)
    IB_EVENT *evh
    CODE:
    RETVAL = evh->cb_called;
    OUTPUT:
    RETVAL

MODULE = DBD::InterBase     PACKAGE = DBD::InterBase::st

char*
ib_plan(sth)
    SV *sth
    CODE:
{
    D_imp_sth(sth);
    ISC_STATUS  status[ISC_STATUS_LENGTH];
    char plan_info[1];
    char plan_buffer[PLAN_BUFFER_LEN];

    RETVAL = NULL;
    memset(plan_buffer, 0, PLAN_BUFFER_LEN);
    plan_info[0] = isc_info_sql_get_plan;

    if (isc_dsql_sql_info(status, &(imp_sth->stmt), sizeof(plan_info), plan_info,
                  sizeof(plan_buffer), plan_buffer)) 
    {
        if (ib_error_check(sth, status))
        {
            ib_cleanup_st_prepare(imp_sth);
            XSRETURN_UNDEF;
        }
    }
    if (plan_buffer[0] == isc_info_sql_get_plan) {
        short l = (short) isc_vax_integer((char *)plan_buffer + 1, 2);
        if ((RETVAL = (char*)safemalloc(sizeof(char) * (l + 2))) == NULL) {
            do_error(sth, 2, "Failed to allocate plan buffer");
            XSRETURN_UNDEF;
        }
        sprintf(RETVAL, "%.*s%s", l, plan_buffer + 3, "\n");
        //PerlIO_printf(PerlIO_stderr(), "Len: %d, orig len: %d\n", strlen(imp_sth->plan), l);
    }
}
    OUTPUT:
    RETVAL

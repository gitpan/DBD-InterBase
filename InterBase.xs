/*
   $Id: InterBase.xs,v 1.23 2002/04/04 09:50:16 edpratomo Exp $

   Copyright (c) 1999-2002  Edwin Pratomo

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.

*/

#include "InterBase.h"

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

    if (dbis->debug >= 1)
    {
       PerlIO_printf(DBILOGFP, "db::_do\n");
       PerlIO_printf(DBILOGFP, "Executing : %s\n", sbuf);
    }

    /* we need an open transaction */
    if (!imp_dbh->tr)
    {
        if (dbis->debug >= 1)
            PerlIO_printf(DBILOGFP, "starting new transaction..\n");

        if (!ib_start_transaction(dbh, imp_dbh))
        {
            retval = -2;
            XST_mUNDEF(0);      /* <= -2 means error        */
            return;
        }
        if (dbis->debug >= 1)
            PerlIO_printf(DBILOGFP, "new transaction started.\n");
    }

    /* only execute_immediate statment if NOT in AutoCommit mode */
    if (!is_softcommit(imp_dbh))
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
    int ret;
    ret = dbd_db_ping(dbh);
    if (ret == 0)
        XST_mUNDEF(0);
    else
        XST_mIV(0, ret);

void
set_tx_param(dbh, ...)
    SV *dbh
    PREINIT:
    STRLEN len;
    char   *tx_key, *tx_val, *tpb, *tmp_tpb;
    int    i, ret, rc = 0;
    int    tpb_len;
    char   am_set = 0, il_set = 0, ls_set = 0;
    I32    j;
    AV     *av;
    HV     *hv;
    SV     **svp, *sv;
    HE     *he;

    CODE:
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
        if (strEQ(SvPV_nolen(ST(i)), "-reserving"))
            if (SvROK(ST(i + 1)) && SvTYPE(SvRV(ST(i + 1))) == SVt_PVHV)
            {
                hv = (HV *)SvRV(ST(i + 1));
                hv_iterinit(hv);
                while (he = hv_iternext(hv))
                {
                    /* retrieve the size of table name(s) */
                    HePV(he, len);
                    tpb_len += len + 4;
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
        tx_key = SvPV_nolen(ST(i));

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

            tx_val = SvPV_nolen(ST(i + 1));
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

            if (SvROK(ST(i + 1)) && SvTYPE(SvRV(ST(i + 1))) == SVt_PVAV)
            {
                av = (AV *)SvRV(ST(i + 1));

                /* sanity check */
                for (j = 0; (j <= av_len(av)) && !rc; j++)
                {
                    svp = av_fetch(av, j, FALSE);
                    if (strEQ(SvPV_nolen(*svp), "read_committed"))
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
                tx_val = SvPV_nolen(ST(i + 1));
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

            tx_val = SvPV_nolen(ST(i + 1));
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
            if (SvROK(ST(i + 1)) && SvTYPE(SvRV(ST(i + 1))) == SVt_PVHV)
            {
                char *table_name;
                HV *table_opts;
                hv = (HV *)SvRV(ST(i + 1));
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
                            int k;
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

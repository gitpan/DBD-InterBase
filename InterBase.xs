/* 
   $Id: InterBase.xs,v 1.15 2001/08/01 00:49:30 danielritz Exp $ 

   Copyright (c) 1999-2001  Edwin Pratomo

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
    ISC_STATUS  status[ISC_STATUS_LENGTH]; /* isc api status vector    */
    STRLEN slen;
    int retval;
    char* sbuf = SvPV(statement, slen);

    if (dbis->debug >= 1) 
    {
       PerlIO_printf(DBILOGFP, "db::_do\n");
       PerlIO_printf(DBILOGFP, "Executing : %s\n", sbuf);
    }

    if (!imp_dbh->tr)
    {
        if (dbis->debug >= 1) 
        {
            PerlIO_printf(DBILOGFP, "starting new transaction..\n");
        }

        if (!ib_start_transaction(dbh, imp_dbh, NULL, 0)) 
        {
            retval = -2;
            XST_mUNDEF(0);      /* <= -2 means error        */
            return;
        }
        if (dbis->debug >= 1) 
        {
            PerlIO_printf(DBILOGFP, "new transaction started.\n");
        }
    }

    isc_dsql_execute_immediate(status, &(imp_dbh->db), &(imp_dbh->tr),
        0, sbuf, imp_dbh->sqldialect, NULL);

    if (ib_error_check(dbh, status))
        retval = -2;
    else 
        retval = -1 ;

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
    if (ret == 0) {
        XST_mUNDEF(0);
    }
    else {
        XST_mIV(0, ret);
    }

void
set_tx_param(dbh, ...)
    SV *dbh
    PREINIT:
    STRLEN len;
    char *tx_key, *tx_val, *tpb, *tmp_tpb;
    int i, ret, rc = 0;
    I32 j;
    AV *av;
    HV *hv;
    SV **svp, *sv;

    CODE:
    D_imp_dbh(dbh);
    tmp_tpb = (char*)safemalloc(512 * sizeof(char));
    if (tmp_tpb == NULL) 
        croak("set_tx_param: Can't alloc memory");

    tpb = tmp_tpb;
    *tpb++ = isc_tpb_version3;
    
    for (i = 1; i < items; i += 2) 
    {
      tx_key = SvPV(ST(i), len);
      if (strEQ(tx_key, "-access_mode")) 
      {
        tx_val = SvPV(ST(i + 1), len);
        if (strEQ(tx_val, "read_write")) 
        {
          *tpb++ = isc_tpb_write;             
        } 
        else if (strEQ(tx_val, "read_only")) 
        {
          *tpb++ = isc_tpb_read;
        } 
        else 
        {
          safefree(tmp_tpb);
          croak("Unknown transaction parameter %s", tx_val);
        }
      } 
      else if (strEQ(tx_key, "-isolation_level")) 
      {
        if (SvROK(ST(i + 1)) && SvTYPE( SvRV(ST(i + 1)) ) == SVt_PVAV) 
        {
          av = (AV*)SvRV(ST(i + 1));

          /* sanity check */
          for (j = 0; j <= av_len(av); j++) 
          {
            svp = av_fetch(av, j, FALSE);
            if ( strEQ( SvPV_nolen(*svp), "read_committed" )) 
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
            svp = av_fetch(av, j, FALSE);
            if ( strEQ( SvPV_nolen(*svp), "record_version" )) 
            {
              *tpb++ = isc_tpb_rec_version;
            } 
            else if (strEQ( SvPV_nolen(*svp), "no_record_version" )) 
            {
              *tpb++ = isc_tpb_no_rec_version;
            }
          }
        } 
        else 
        {
          tx_val = SvPV(ST(i + 1), len);
          if (strEQ(tx_val, "read_committed")) 
          {
            *tpb++ = isc_tpb_read_committed;
          } 
          else if (strEQ(tx_val, "snapshot")) 
          {
            *tpb++ = isc_tpb_concurrency;
          } 
          else if (strEQ(tx_val, "snapshot_table_stability")) 
          {
            *tpb++ = isc_tpb_consistency;
          }
        }
      } 
      else if (strEQ(tx_key, "-lock_resolution")) 
      {
        tx_val = SvPV(ST(i + 1), len);
        if (strEQ(tx_val, "wait")) 
        {
          *tpb++ = isc_tpb_wait;
        } 
        else if (strEQ(tx_val, "no_wait")) 
        {
          *tpb++ = isc_tpb_nowait;
        } 
        else 
        {
          safefree(tmp_tpb);
          croak("Unknown transaction parameter %s", tx_val);
        }
      } 
      else if (strEQ(tx_key, "-reserving")) 
      {
        if (SvROK(ST(i + 1)) && SvTYPE( SvRV(ST(i + 1)) ) == SVt_PVAV) 
        {
          av = (AV*)SvRV(ST(i + 1));
          for (j = 0; j < av_len(av); j++) 
          {
            sv = *av_fetch(av, j, FALSE);
            if (SvROK(sv) && SvTYPE(sv) == SVt_PVHV) 
            {
              hv = (HV*)SvRV(sv);
              sv = *hv_fetch(hv, "for", 3, FALSE);

              if (strnEQ(SvPV(sv, PL_na), "shared", 6)) 
              {
                *tpb++ = isc_tpb_shared;
              }

              if (strnEQ(SvPV(sv, PL_na), "protected", 9)) 
              {
                *tpb++ = isc_tpb_protected;
              }

              sv = *hv_fetch(hv, "table", 5, FALSE);
              /* unfinished 
                            
              */                          
            } 
            else 
            {
              safefree(tmp_tpb);
              croak("Invalid -reserving hashref value");
            }
          }
        } 
        else 
        {
          safefree(tmp_tpb);
          croak("Invalid -reserving value");
        }
      } 
      else 
      {
        safefree(tmp_tpb);
        croak("Unknown transaction parameter %s", tx_key);
      }
    }

    safefree(imp_dbh->tpb_buffer);
    imp_dbh->tpb_buffer = tmp_tpb;
    imp_dbh->tpb_length = tpb - imp_dbh->tpb_buffer;


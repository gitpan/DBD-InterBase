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

    if (DBIc_has(imp_dbh, DBIcf_AutoCommit)) 
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
            retval = FALSE;
    }

    XST_mIV(0, retval); /* typically 1, rowcount or -1  */
}

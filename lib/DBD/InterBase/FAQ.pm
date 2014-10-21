### $Id: FAQ.pm,v 1.2 2001/04/27 16:19:20 edpratomo Exp $
### DBD::InterBase Frequently Asked Questions POD
### 
### This document is Copyright (c)2000 Edwin Pratomo. All rights reserved.
### Permission to distribute this document, in full or in part, via email,
### Usenet, ftp archives or http is granted providing that no charges are involved,
### reasonable attempt is made to use the most current version and all credits
### and copyright notices are retained ( the I<AUTHOR> and I<COPYRIGHT> sections ).
### Requests for other distribution rights, including incorporation into 
### commercial products, such as books, magazine articles or CD-ROMs should be
### made to Edwin Pratomo <I<ed.pratomo@computer.org>>.
###
### This module is released under
### the 'Artistic' license which you can find in the perl distribution.
### 

package DBD::InterBase::FAQ;

=head1 NAME

FAQ - The Frequently Asked Questions for C<DBD::InterBase>

=head1 SYNOPSIS

perldoc DBD::InterBase::FAQ

=head1 DESCRIPTION

This document serves to answer the most frequently asked questions
regarding the uses of C<DBD::InterBase>. Current version refers to
C<DBD::InterBase> version 0.26 available in CPAN.

=head1 SQL Operations

=head2 Why do some operations performing positioned update and delete fail when AutoCommit is on? 

The following code snippet fails:

 $sth = $dbh->prepare(
 "SELECT * FROM ORDERS WHERE user_id < 5 FOR UPDATE OF comment");
 $sth->execute;
 while (@res = $sth->fetchrow_array) {
     $dbh->do("UPDATE ORDERS SET comment = 'Wonderful' WHERE 
     CURRENT OF $sth->{CursorName}");
 }

When B<AutoCommit is on>, a transaction is started within prepare(), and
committed automatically after the last fetch(), or within finish(). Within
do(), a transaction is started right before the statement is executed, and
gets committed right after the statement is executed. The transaction handle
is stored within the database handle. The driver is smart enough not to
override an active transaction handle with a new one. So, if you notice the
snippet above, after the first fetchrow_array(), the do() is still using the
same transaction context, but as soon as it has finished executing the statement, it
B<commits> the transaction, whereas the next fetchrow_array() still needs
the transaction context!

The workaround for this is to set AutoCommit = 0. If yours is default to
AutoCommit on, you can put the snippet within a block:

 {
     $dbh->{AutoCommit} = 0;
     # same actions like above ....
     $dbh->commit;
 }

=head2 Why do placeholders fail to bind, generating unknown datatype error message?

You can't bind a field name. The following example will fail:

 $sth = $dbh->prepare("SELECT (?) FROM $table");
 $sth->execute('user_id');

There are cases where placeholders can't be used in conjunction with COLLATE
clause, such as this:

 SELECT * FROM $table WHERE UPPER(author) LIKE UPPER(? COLLATE FR_CA);

This deals with the InterBase's SQL parser, not with C<DBD::InterBase>. The
driver just passes SQL statements through the engine.

=head2 How to do automatic increment for a specific field?

Create a generator and a trigger to associate it with the field. The
following example creates a generator named PROD_ID_GEN, and a trigger for
table ORDERS which uses the generator to perform auto increment on field
PRODUCE_ID with increment size of 1.

 $dbh->do("CREATE GENERATOR PROD_ID_GEN");
 $dbh->do(
 "CREATE TRIGGER INC_PROD_ID FOR ORDERS
 BEFORE INSERT POSITION 0
 AS BEGIN
   NEW.PRODUCE_ID = GEN_ID(PROD_ID_GEN, 1);
 END");

=head2 How can I perform LIMIT clause as I usually do in MySQL?

C<LIMIT> clause let users to fetch only a portion rather than the whole 
records as the result of a query. This is particularly efficient and useful 
for paging feature on web pages, where users can navigate back and forth 
between pages. Using InterBase, this can be emulated by writing a stored 
procedure. For example, to display a portion of table_forum, first create the 
following procedure:

 CREATE PROCEDURE PAGING_FORUM (start INTEGER, num INTEGER)
 RETURNS (id INTEGER, title VARCHAR(255), ctime DATE, author VARCHAR(255))
 AS 
 DECLARE VARIABLE counter INTEGER;
 BEGIN
   counter = 0;
   FOR SELECT id, title, ctime, author FROM table_forum ORDER BY ctime
      INTO :id, :title, :ctime, :author
   DO
   BEGIN
      IF (counter = :start + :num) THEN EXIT;
      ELSE
         IF (counter >= :start) THEN SUSPEND;
      counter = counter + 1;          
   END
 END !!
 SET TERM ; !!

And within your application:

 # fetch record 1 - 5:
 $res = $dbh->selectall_arrayref("SELECT * FROM paging_forum(0,5)");

 # fetch record 6 - 10: 
 $res = $dbh->selectall_arrayref("SELECT * FROM paging_forum(5,5)");

But never expect this to work:

 $sth = $dbh->prepare(
 "EXECUTE PROCEDURE paging_forum(5,5) RETURNING_VALUES :id, :title, :ctime, 
 :author");

=head1 Uses of attributes

=head2 How can I use the date/time formatting attributes?

Those attributes take the same format as the C function strftime()'s.
Examples:

 $attr = {
    ib_timestampformat => '%m-%d-%Y %H:%M',
    ib_dateformat => '%m-%d-%Y',
    ib_timeformat => '%H:%M',
 };

Then, pass it to prepare() method. 

 $sth = $dbh->prepare($stmt, $attr);
 # followed by execute() and fetch(), or:

 $res = $dbh->selectall_arrayref($stmt, $attr);

=head2 Can I set the date/time formatting attributes between prepare and fetch?

No. C<ib_dateformat>, C<ib_timeformat>, and C<ib_timestampformat> can only
be set during $sth->prepare. If this is a problem to you, let me know, and
probably I'll add this capability for the next release.

=head2 Can I change ib_dialect after DBI->connect ?

No. If this is a problem to you, let me know, and probably I'll add this 
capability for the next release.

=head2 Why do execute(), do() method and rows() method always return -1 upon 
a successful operation?

Incorrect question. $sth->rows returns the number of fetched rows after a
successful SELECT. But it's true that for statements other than SELECT, it 
returns -1, because I don't know how to get the number of affected rows :-}

=head1 Sources for Help

=head2 I can't find the answer for my question here, where should I direct my question?

For questions regarding InterBase itself, you can join the InterBase mailing
list at http://www.mers.com/, or if it is not enough, I believe there are
some commercial supports available out there. http://www.ibphoenix.com/ is a
good place to check.

For questions about C<DBD::InterBase>, try to look for the answer on C<DBI>
man page, and C<DBI::FAQ>. If your question is still unanswered, you can
drop me message or you can post your question to the DBI users mailing list.

=head1 The Development Project

=head2 How can I join the development project?

The project is hosted at sourceforge.net. So send me your sourceforge
username, and let me know what areas you are interested in. 

=head2 Is there a mailing list for DBD::InterBase?

Please join dbi-interbase-devel mailing list at 
http://lists.sourceforge.net/mailman/listinfo/dbi-interbase-devel/

=head2 Where can I get the latest release of DBD::InterBase ?

http://dbi.interbase.or.id/ (stable and development release), and 
http://www.cpan.org/modules/by-module/DBD/ (stable release only).

=head1 AUTHOR AND COPYRIGHT

Copyright (C) 2000-2001, Edwin Pratomo I<ed.pratomo@computer.org>.
Michael Samanov I<samanov@yahoo.com> contributed some important correction.

=cut

#!/usr/local/bin/perl -w
#
#   $Id: 70nestedoff.t,v 1.2 2002/04/05 03:12:51 edpratomo Exp $
#
#   This is a test for nested statement handles.
#


#
#   Make -w happy
#
$test_dsn = '';
$test_user = '';
$test_password = '';

my $rec_num = 2;

#
#   Include lib.pl
#
#use strict;
use DBI;
use vars qw($verbose);

my ($dbh, $sth1, $sth2, $row, $res);

$mdriver = "";
foreach $file ("lib.pl", "t/lib.pl") {
    do $file; if ($@) { print STDERR "Error while executing lib.pl: $@\n";
               exit 10;
              }
    if ($mdriver ne '') {
    last;
    }
}

sub ServerError() {
    print STDERR ("Cannot connect: ", $DBI::errstr, "\n",
    "\tEither your server is not up and running or you have no\n",
    "\tpermissions for acessing the DSN $test_dsn.\n",
    "\tThis test requires a running server and write permissions.\n",
    "\tPlease make sure your server is running and you have\n",
    "\tpermissions, then retry.\n");
    exit 10;
}

while (Testing()) {
    #
    #   Connect to the database
    Test($state or $dbh = DBI->connect($test_dsn, $test_user, $test_password))
    or ServerError();

    #
    #   Find a possible new table name
    #
     Test($state or $table = FindNewTable($dbh))
       or DbiError($dbh->err, $dbh->errstr);

    #
    #   Create a new table
    #
    my $def;
    unless ($state) {
        $def = "CREATE TABLE $table(id INTEGER, name VARCHAR(20))";
    }

    Test($state or $dbh->do($def))
    or DbiError($dbh->err, $dbh->errstr);

    Test($state or $dbh->do("INSERT INTO $table"
                . " VALUES(1, 'Crocodile')"))
    or DbiError($dbh->err, $dbh->errstr);

	#DBI->trace(4, "trace.txt");
	{
		local $dbh->{AutoCommit} = 0;

		Test($state or $sth1 = $dbh->prepare("SELECT * FROM $table"))
		or DbiError($dbh->err, $dbh->errstr);

		Test($state or $sth2 = $dbh->prepare("SELECT * FROM $table WHERE id = ?"))
		or DbiError($dbh->err, $dbh->errstr);

		Test($state or $sth1->execute)
		or DbiError($sth1->err, $sth1->errstr);

		unless ($state) {
			while ($row = $sth1->fetchrow_arrayref) {

				Test($sth2->execute($row->[0])) 
				or DbiError($sth2->err, $sth2->errstr);

				Test(@{$res = $sth2->fetchall_arrayref})
				or DbiError($sth2->err, $sth2->errstr);
			}
		} else { for (1..2) { Test($state) } }

		Test($state or $dbh->commit)
		or DbiError($dbh->err, $dbh->errstr);

    #
    #  Drop the test table
    #
    Test($state or $dbh->do("DROP TABLE $table"))
       or DbiError($dbh->err, $dbh->errstr);

		Test($state or $dbh->commit)
		or DbiError($dbh->err, $dbh->errstr);

	}


    #
    #   Finally disconnect.
    #
    Test($state or $dbh->disconnect())
       or DbiError($dbh->err, $dbh->errstr);

}

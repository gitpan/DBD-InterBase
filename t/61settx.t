#!/usr/local/bin/perl -w
#
#   $Id: 61settx.t,v 1.1 2001/03/24 14:02:41 edpratomo Exp $
#
#   This is a test for set_tx_param() private method.
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
use DBI;
use vars qw($verbose);



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
    Test($state or $dbh1 = DBI->connect($test_dsn, $test_user, $test_password))
    or ServerError();

    Test($state or $dbh2 = DBI->connect($test_dsn, $test_user, $test_password))
    or ServerError();

    #
    #   Find a possible new table name
    #
     Test($state or $table = FindNewTable($dbh1))
       or DbiError($dbh1->err, $dbh1->errstr);

    #
    #   Create a new table
    #
    my $def;
    unless ($state) {
        $def = "CREATE TABLE $table(id INTEGER, name VARCHAR(20))";
    }

    Test($state or $dbh1->do($def))
    or DbiError($dbh1->err, $dbh1->errstr);

    #
    #   Changes transaction params
    #
    Test($state or $dbh1->func( 
        -access_mode     => 'read_write',
        -isolation_level => 'read_committed',
        -lock_resolution => 'wait',
        'set_tx_param'))
    or DbiError($dbh1->err, $dbh1->errstr);

    Test($state or $dbh2->func(
        -isolation_level => 'snapshot_table_stability',
        -lock_resolution => 'no_wait',
        'set_tx_param'))
    or DbiError($dbh2->err, $dbh2->errstr);

    DBI->trace(1, "trace.txt");
    {
        local $dbh1->{AutoCommit} = 0;
        local $dbh2->{PrintError} = 0;

        my ($stmt, $stmt2);
        unless ($state) {
            $stmt = "INSERT INTO $table VALUES(?, 'Yustina')";
#           $stmt = "INSERT INTO $table VALUES(1, 'Yustina')";
#           $stmt2 = "INSERT INTO $table VALUES(2, 'Yustina')";
        }

        Test($state or $dbh1->do($stmt, undef, 1))
            or DbiError($dbh1->err, $dbh1->errstr);

        # expected failure:
        Test($state or not $dbh2->do($stmt, undef, 2))
            or DbiError($dbh2->err, $dbh2->errstr);
    }

    #
    #  Drop the test table
    #
    Test($state or $dbh1->do("DROP TABLE $table"))
       or DbiError($dbh1->err, $dbh1->errstr);

    #
    #   Finally disconnect.
    #
    Test($state or $dbh1->disconnect())
       or DbiError($dbh1->err, $dbh1->errstr);

    Test($state or $dbh2->disconnect())
       or DbiError($dbh2->err, $dbh2->errstr);
}

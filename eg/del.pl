#!/usr/bin/perl -w
require 5.004;

use blib;
use DBI;

$dbpath = 'test.gdb';
#DBI->trace(2, "./del.trace");
$dbh = DBI->connect("dbi:InterBase:database=$dbpath;ib_dialect=3",'','',
	{AutoCommit => 0}) or die $DBI::errstr;

$sql = 'DELETE FROM SIMPLE';
$cursor = $dbh->prepare($sql) or die $dbh->errstr;

print "Deleting records...\n";
$cursor->execute or die $dbh->errstr;

print "Finished.\n";
$dbh->commit;
$dbh->disconnect or warn $dbh->errstr;


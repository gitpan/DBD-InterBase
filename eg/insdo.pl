#!/usr/bin/perl -w
require 5.004;

use blib;
use DBI;

$dbpath = 'test.gdb';
#DBI->trace(2, "./insdo.trace");
$dbh = DBI->connect("dbi:InterBase:database=$dbpath;ib_dialect=3",'','', 
	{AutoCommit => 0}) or die $DBI::errstr;

$sql =<< "EOS"; 
INSERT INTO SIMPLE VALUES (GEN_ID(gen_person_id,1), 'Gurusamy Sarathy',
'Main porter', 'NOW')
EOS

$dbh->do($sql) or die $dbh->errstr;

$dbh->commit;
$dbh->disconnect or warn $dbh->errstr;

print "Added one record.\n";

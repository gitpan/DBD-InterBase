#!/usr/bin/perl -w
require 5.004;

use blib;
use DBI;

$dbpath = 'test.gdb';
#DBI->trace(2, "./del.trace");
$dbh = DBI->connect("dbi:InterBase:database=$dbpath",'','',{AutoCommit => 0}) 
    or die $DBI::errstr;

$sql = 'delete from SIMPLE where person_id = ?';
$cursor = $dbh->prepare($sql) or die $dbh->errstr;

print "Deleting records...\n";
for (1..6)
{
    $cursor->execute($_) or die $dbh->errstr;
}
print "Finished.\n";
$dbh->commit;
$dbh->disconnect or warn $dbh->errstr;


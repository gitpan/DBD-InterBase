#!/usr/bin/perl -w
require 5.004;

use blib;
use DBI;

$dbpath = 'test.gdb';
$dbh = DBI->connect("dbi:InterBase:database=$dbpath") or die $DBI::errstr;

$cursor = $dbh->prepare("SELECT * FROM SIMPLE ORDER BY PERSON_ID");
$cursor->execute;

format STDOUT_TOP =
  ID PERSON                    COMMENT
----------------------------------------------------------
.

format STDOUT =
@>>> @<<<<<<<<<<<<<<<<<<<<<<<< ^<<<<<<<<<<<<<<<<<<<<<<<<<<
$row[0],$row[1],$row[2]
~~                             ^<<<<<<<<<<<<<<<<<<<<<<<<<<              
$row[2]

.

while (@row = $cursor->fetchrow_array) {
    write;
}

$dbh->disconnect;
__END__

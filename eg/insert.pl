#!/usr/bin/perl -w
require 5.004;

use blib;
use DBI;

$dbpath = 'test.gdb';
#DBI->trace(2, "./insert.trace");
$dbh = DBI->connect("dbi:InterBase:database=$dbpath;ib_dialect=3",'','') 
    or die $DBI::errstr;

@params = ([1, "Tim Bunce", "Creator of DBI", 'NOW'],
           [2, "Jonathan Leffler", "Creator of DBD::Informix", 'NOW'],
           [3, "Larry Wall", "Creator of Perl", 'NOW'],
           [4, "Steven Haryanto", 'Bandung main mangler :-)', 'NOW'],

           [6, "Yustina Sri Suharini", 'My fiance', 'NOW']);

$sql = 'INSERT INTO SIMPLE VALUES(GEN_ID(gen_person_id,1), ?, ?, ?)';
$cursor = $dbh->prepare($sql) or die $dbh->errstr;

print "Inserting records...\n";
for (@params)
{
    $cursor->execute(${@$_}[1], ${@$_}[2], ${@$_}[3])
        or die $dbh->errstr;
}
#print "Jumlah baris terpengaruh: ", $cursor->rows, "\n";
print "Finished.\n";
$dbh->disconnect or warn $dbh->errstr;

__END__

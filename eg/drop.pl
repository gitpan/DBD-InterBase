#!/usr/bin/perl -w

use blib;
use DBI;

$dsn = 'dbi:InterBase:database=test.gdb';
$dbh = DBI->connect($dsn, '', '', {RaiseError => 1})
	or die "$DBI::errstr";
$stmt = "drop table PERSON";
$dbh->do($stmt);
$dbh->disconnect;

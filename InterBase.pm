
use strict;

use Carp;
use IBPerl;

package DBD::InterBase;

use vars qw($VERSION $err $errstr $drh);

$VERSION = '0.1';

$err = 0;
$errstr = "";
$drh = undef;

sub driver
{
    return $drh if $drh;
    my($class, $attr) = @_;

    $class .= "::dr";

    $drh = DBI::_new_drh($class, { 'Name' => 'InterBase',
                   'Version' => $VERSION,
                   'Err'    => \$DBD::InterBase::err,
                   'Errstr' => \$DBD::InterBase::errstr,
                   'Attribution' => 'DBD::InterBase by Edwin Pratomo'
                 });
    $drh;
}

#############   
# DBD::InterBase::dr
# methods:
#   connect
#   disconnect_all
#   DESTROY

package DBD::InterBase::dr;

$DBD::InterBase::dr::imp_data_size = $DBD::InterBase::dr::imp_data_size = 0;
$DBD::InterBase::dr::data_sources_attr = $DBD::InterBase::dr::data_sources_attr = undef;

sub connect {
    my($drh, $dsn, $dbuser, $dbpasswd, $attr) = @_;
    my %conn;
    my ($key, $val);

    foreach my $pair (split(/;/, $dsn))
    {
        ($key, $val) = $pair =~ m{(.+)=(.*)};
        if ($key eq 'host') { $conn{Server} = $val}
        elsif ($key =~ m{database}) { $conn{Path} = $val }
        elsif ($key =~ m{ib_protocol}i) { $conn{Protocol} = $val }
        elsif ($key =~ m{ib_role}i) {  $conn{Role} = $val }
        elsif ($key =~ m{ib_charset}i) { $conn{Charset} = $val }
        elsif ($key =~ m{ib_cache}i) {$conn{Cache} = $val }
        elsif ($key =~ m{ib_dialect}i) {$conn{Dialect} = int($val) }
    }

    $conn{User} = $dbuser || "SYSDBA";
    $conn{Password} = $dbpasswd || "masterkey";
        
    my $db = new IBPerl::Connection(%conn);
    if ($db->{Handle} < 0) {
	    $drh->DBI::set_err(-1, $db->{Error});
        return undef;
    }

    my $h = new IBPerl::Transaction(Database=>$db);
    if ($h->{Handle} < 0) {
	    $drh->DBI::set_err(-1, $db->{Error});
        return undef;
    }

    my $this = DBI::_new_dbh($drh, {
        'Name' => $dsn,
        'User' => $dbuser, 
    });

    if ($this)
    {
        while (($key, $val) = each(%$attr))
        {
            $this->STORE($key, $val);   #set attr like AutoCommit
        }
    }

    $this->STORE('ib_conn_handle', $db);
    $this->STORE('ib_trans_handle', $h);
    $this->STORE('Active', 1);
    $this;
}

sub disconnect_all { }

sub DESTROY { undef; }


##################
# DBD::InterBase::db
# methods:
#   prepare
#   commit
#   rollback
#   disconnect
#   do
#   ping
#   STORE
#   FETCH
#   DESTROY

package DBD::InterBase::db;

$DBD::InterBase::db::imp_data_size = $DBD::InterBase::db::imp_data_size= 0;

sub prepare 
{
    my($dbh, $statement, $attribs)= @_;
    my $h = $dbh->FETCH('ib_trans_handle');

    if (!$h)
    {
        return $dbh->DBI::set_err(-1, "Fail to get transaction handle");
    }

    $attribs->{ib_separator} = undef unless exists ($attribs->{ib_separator});
    $attribs->{ib_dateformat} = "%c" unless exists ($attribs->{ib_dateformat});

    my $st = new IBPerl::Statement(
        Transaction => $h,
        Stmt => $statement,
        Separator => $attribs->{ib_separator},
        DateFormat => $attribs->{ib_dateformat},
    );

    if ($st->{Handle} < 0) {
        return $dbh->DBI::set_err(-1, $st->{Error});
    }

    my $sth = DBI::_new_sth($dbh, {'Statement' => $statement});

    if ($sth) {
        $sth->STORE('ib_stmt_handle', $st);
        $sth->STORE('ib_stmt', $statement);
        $sth->STORE('ib_params', []); #storing bind values
        $sth->STORE('NUM_OF_PARAMS', ($statement =~ tr/?//));
    }
    $sth;
}

sub _commit
{
    my $dbh = shift;
    my $db = $dbh->FETCH('ib_conn_handle');
    my $h = $dbh->FETCH('ib_trans_handle');
    if ($h->IBPerl::Transaction::commit < 0) {
        return $dbh->DBI::set_err(-1, $h->{Error});
    }
    $h = new IBPerl::Transaction(Database => $db);
    if ($h->{Handle} < 0) {
        return $dbh->DBI::set_err(-1, $h->{Error});
    }
    $dbh->STORE('ib_trans_handle', $h);

    1;
}

sub commit
{
    my $dbh = shift;

    if ($dbh->FETCH('AutoCommit')) {
        warn("Commit ineffective while AutoCommit is on");
    }
    else { return _commit($dbh); }
    return 1;
}

sub rollback
{
    my $dbh = shift;
    if ($dbh->FETCH('AutoCommit')) {
        warn("Rollback ineffective while AutoCommit is on");
    }
    else
    {
        my $h = $dbh->FETCH('ib_trans_handle');
        if ($h->IBPerl::Transaction::rollback < 0) {
            return $dbh->DBI::set_err(-1, $h->{Error});
        }   
    }
    1;
}

sub disconnect
{
    my $dbh = shift;
    my $db = $dbh->FETCH('ib_conn_handle');
    my $h = $dbh->FETCH('ib_trans_handle');
    if ($dbh->FETCH('AutoCommit'))
    {
        if ($h->IBPerl::Transaction::commit < 0) {
            return $dbh->DBI::set_err(-1, $h->{Error});
        }
    }
    else
    {
        if ($h->IBPerl::Transaction::rollback < 0) {
            return $dbh->DBI::set_err(-1, $h->{Error});
        }
    }
    my $retval = $db->IBPerl::Connection::disconnect;
    if ($retval < 0)
    {
        return $dbh->DBI::set_err(-1, $db->{Error});
    }   
    $dbh->STORE('Active', 0);
    1;
}

sub do
{
    my ($dbh, $stmt, $attr, @params) = @_;
    my $sth = $dbh->prepare($stmt, $attr) or return undef;
    my $st = $sth->{'ib_stmt_handle'};
    if ($st->IBPerl::Statement::execute(@params) < 0)
    {
        return $sth->DBI::set_err(1, $st->{Error});
    }   
    _commit($dbh) if ($dbh->{AutoCommit});
    -1;
}

sub tables
{
    my $dbh = shift;
    my @tables;
    my %row;
    my $statement = "SELECT RDB\$RELATION_NAME FROM RDB\$RELATIONS WHERE (RDB\$SYSTEM_FLAG IS NULL OR RDB\$SYSTEM_FLAG = 0) AND RDB\$VIEW_SOURCE
IS NULL";
    my $h = $dbh->FETCH('ib_trans_handle');
 
    if (!$h)
    {
        return $dbh->DBI::set_err(-1, "Fail to get transaction handle");
    }
 
    my $st = new IBPerl::Statement(
        Transaction => $h,
        Stmt => $statement,
    );
 
    if ($st->{Handle} < 0) {
        return $dbh->DBI::set_err(-1, $st->{Error});
    }
 
    if ($st->open() < 0) {
        return $dbh->DBI::set_err(-1, $st->{Error});
    }
 
    while ($st->IBPerl::Statement::fetch(\%row)==0) {
        foreach (keys %row) {
           $row{$_} =~ s/\s+$//;
           push(@tables, $row{$_});
        }
    }
    return @tables;
}


sub STORE
{
    my ($dbh, $attr, $val) = @_;
    if ($attr eq 'AutoCommit')
    {
        if (exists $dbh->{AutoCommit} and $val == 1 and $dbh->{$attr} == 0) 
        {
            _commit($dbh) or 
                warn("Problem encountered while setting AutoCommit to On");
        }
        $dbh->{$attr} = $val;
        return 1;
    }
    if ($attr =~ /^ib_/)
    {
        $dbh->{$attr} = $val;
        return 1;
    }
    $dbh->DBD::_::db::STORE($attr, $val);
}

sub FETCH
{
    my ($dbh, $attr) = @_;
    if ($attr eq 'AutoCommit')
    {
        return $dbh->{$attr};
    }
    if ($attr =~ /^ib_/)
    {
        return $dbh->{$attr};
    }
    $dbh->DBD::_::db::FETCH($attr);
}

sub DESTROY
{
    my $dbh = shift;
    $dbh->disconnect if $dbh->FETCH('Active');
    undef;
}

sub ping {
    my($dbh) = @_;
    my $ret = 0;
    eval {
            local $SIG{__DIE__}  = sub { return (0); };
            local $SIG{__WARN__} = sub { return (0); };
            # adapt the select statement to your database:
            my $sth = $dbh->prepare('SELECT 1 FROM RDB$DATABASE');
            $ret = $sth && ($sth->execute);
            $sth->finish;
    };
    return ($@) ? 0 : $ret;
}

####################
#
# DBD::InterBase::st
# methods:
#   execute
#   fetchrow_arrayref
#   finish  
#   STORE
#   FETCH
#   DESTROY 
#
####################

package DBD::InterBase::st;
use strict;
$DBD::InterBase::st::imp_data_size = $DBD::InterBase::st::imp_data_size = 0;

sub bind_param
{
    my ($sth, $pNum, $val, $attr) = @_;
    my $type = (ref $attr) ? $attr->{TYPE} : $attr;
    if ($type) {
        my $dbh = $sth->{'Database'};
        $val = $dbh->quote($sth, $type);
    }
    $sth->{ib_params}->[$pNum-1] = $val;
    1;
}

sub execute
{
    my ($sth, @bind_values) = @_;
    my $params = (@bind_values) ? \@bind_values : 
                 $sth->FETCH('ib_params');
    my $num_param = $sth->FETCH('NUM_OF_PARAMS');

    if (@$params != $num_param) {
        return $sth->DBI::set_err(1, 'Invalid number of params');       
    }

    my $st = $sth->{'ib_stmt_handle'};
    my $stmt = $sth->{'ib_stmt'};
    my $dbh = $sth->{'Database'};
    # do commit to create new snapshopt. 
    DBD::InterBase::db::_commit($dbh) if ($dbh->{AutoCommit});

# use open() for select and execute() for non-select
# execute procedure doesn't work at IBPerl
    if ($stmt =~ m{^\s*?SELECT}i or 
        $stmt =~ m{^\s*?EXECUTE\s+PROCEDURE}i)
    {
        if ($st->IBPerl::Statement::open(@$params) < 0)
        {
            return $sth->DBI::set_err(1, $st->{Error});
        }
    }
    else
    {
        if ($st->IBPerl::Statement::execute(@$params) < 0)
        {
            return $sth->DBI::set_err(1, $st->{Error});
        }
#       $sth->finish; #not work for non-select
        DBD::InterBase::db::_commit($dbh) if ($dbh->{AutoCommit});
    }
    -1;
}

sub fetch
{
    my $sth = shift;
    my $st = $sth->FETCH('ib_stmt_handle');
    my $record_ref = [];

    my $retval = $st->IBPerl::Statement::fetch($record_ref);
    if ($retval == 0) {
        unless ($sth->{NAME})
        {
            $sth->STORE('NAME', $st->{Columns});
            $sth->STORE('NUM_OF_FIELDS', scalar (@{$sth->{NAME}}));
        }
        $sth->STORE('NULLABLE', $st->{Nulls});

        if ($sth->FETCH('ChopBlanks')) {
            map { $_ =~ s/\s+$//; } @$record_ref;
        }
        return $sth->_set_fbav($record_ref);
#       return $record_ref;
    }

    elsif ($retval < 0) {
        return $sth->DBI::set_err(1, $st->{Error});
    }
    elsif ($retval == 100) {
        $sth->finish;
        return undef;
    }
}

*fetchrow_arrayref = \&fetch;

sub finish
{
    my $sth = shift;
    my $st = $sth->FETCH('ib_stmt_handle');
    if ($st->IBPerl::Statement::close < 0) 
    {
        return $sth->DBI::set_err(-1, $st->{Error});
    }
    $sth->DBD::_::st::finish();
    1;
}

sub STORE
{
    my ($sth, $attr, $val) = @_;
    # read-only attributes... who's responsible?
    if ($attr eq 'NAME' 
#       or $attr eq 'NUM_OF_FIELDS' #must be passed to SUPER::STORE()
#       or $attr eq 'NUM_OF_PARAMS' #same as above
        or $attr eq 'NULLABLE'
        or ($attr =~ /^ib_/o)
    )   
    {
        return $sth->{$attr} = $val;
    }

    $sth->DBD::_::st::STORE($attr, $val);
#   $dbh->SUPER::STORE($attr, $val);
}

sub FETCH
{
    my ($sth, $attr) = @_;
    if ($attr =~ /^ib_/ or $attr eq 'NAME' or $attr eq 'NULLABLE')
    {
        return $sth->{$attr};
    }
    $sth->DBD::_::st::FETCH($attr);
#   $dbh->SUPER::FETCH($attr, $attr);   
}

sub DESTROY { undef; }

1;

__END__

=head1 NAME

DBD::InterBase - DBI driver for InterBase RDBMS server

=head1 SYNOPSIS

  use DBI;
  
  $dbpath = '/home/edwin/perl_example.gdb';
  $dsn = "DBI:InterBase:database=$dbpath;host=puskom-4;ib_dialect=3";

  $dbh = DBI->connect($dsn, '', '', {AutoCommit => 0}) 
    or die "$DBI::errstr";

  $dbh = DBI->connect($dsn, '', '', {RaiseError => 1}) 
    or die "$DBI::errstr";

  $sth = $dbh->prepare("select * from SIMPLE") or die $dbh->errstr;
  $sth->execute;

  while (@row = $sth->fetchrow_array))
  {
    print @row, "\n";
  }

  $dbh->commit or warn $dbh->errstr;
  $dbh->disconnect or warn $dbh->errstr;  

For more examples, see eg/ directory.

=head1 DESCRIPTION

This DBI driver currently is a wrapper around IBPerl, written in pure
perl. It is based on the DBI 1.13 specification dan IBPerl 0.8p2. 

B<Connecting with InterBase-specific optional parameters>

InterBase allows you to connect with specifiying ib_role, ib_protocol, 
ib_cache, ib_charset, ib_separator, ib_dateformat, and ib_dialect. 
These parameters can be passed to InterBase via $dsn of DBI connect 
method. Eg:

  $dsn = 'dbi:InterBase:database=/path/to/data.gdb;ib_dialect=3';

=head1 PREREQUISITE

=over 2

=item * InterBase client

Available at http://www.interbase.com/,

=item * IBPerl 0.8 patch 2, by Bill Karwin

Available at http://www.karwin.com/ibperl/

=back

=head1 INSTALLATION

Run:

  # perl Makefile.PL

Here you will be prompted with some questions due to the database that will 
be used during 'make test'.

  # make
  # make test (optional)

The database you specify when running Makefile.PL should has been existed
before running 'make test', otherwise you will get 'Invalid DBI handle -1'
error message. 

  # make install

=head1 WARNING

InterBase specific behaviour:

=over 2

=item * $sth->{NAME} available after the first fetch

=item * $sth->{NUM_OF_FIELDS} available after the first fetch

=item * $dbh->do() doesn't return number of records affected

=item * $sth->execute() doesn't return number of records

=back

=head1 TESTED PLATFORMS

This module has been tested on Linux (2.2.12-20), IBPerl 0.8p2, 
Perl 5.005_03, to access InterBase SuperServer 6.0 for Linux.

=head1 KNOWN BUGS

This sequence won't work:

  $dbh->do($stmt); #any statement on table TBL
  $dbh->commit;
  $dbh->do("drop table TBL");

Workaround: Change the commit with disconnect, and then connect again. This
bug seems to occurs at IBPerl level. Try some examples in eg/ibperl directory.

=head1 BUG REPORTS

Please send any bug report and patches to dbi-users mailing list
(http://www.isc.org/dbi-lists.html) Any bug report should be accompanied with 
the script that got the problem, and the output of DBI trace method.

=head1 HISTORY

B<Version 0.1, June 28, 2000>

Several connect() attributes specific to InterBase now use ib_ prefix:
ib_protocol, ib_role, ib_charset, ib_charset, and ib_cache.

Several bug fixes and features addition:

=over 4

=item * C<Separator> and C<DateFormat> are obsolete. Use C<ib_separator> and
C<ib_dateformat> instead.

=item * C<ib_dialect> attribute in C<connect()> method.

=item * C<set_err> in C<connect()>, by Mark D. Anderson
<I<mda@discerning.com>>,

=item * C<tables()> method, by Michael Arnett <I<marnett@mediaone.net>>,

=item * C<ping()> method, by Flemming Frandsen <I<dion@swamp.dk>>, 
and Mike Shoyher <I<msh@e-labs.ru>>.

=item * AutoCommit=>0 causes segfault within mod_perl, by Mike Shoyher 
<I<msh@e-labs.ru>>.

=back

B<Version 0.021, September 19, 1999>

Separator and DateFormat options works for prepare() and do(). bind_param()
now works. One more fix to AutoCommit behaviour.

B<Version 0.02, July 31, 1999>

Alpha code. Major enhancement from the previous pre-alpha code. Previous 
known problems have been fixed. AutoCommit attribute works as expected. 

B<Version 0.01, July 23, 1999>

Pre-alpha code. An almost complete rewrite of DBIx::IB in pure perl. Problems
encountered during handles destruction phase.

B<DBIx::IB Version 0.01, July 22, 1999>

DBIx::IB, a DBI emulation layer for IBPerl is publicly announced.

=head1 TODO

=over 2

=item * Rigorous test under mod_perl and Apache::DBI

=item * An xs version should be much powerful, and simplify the installation 
process.

=back

=head1 ACKNOWLEDGEMENTS

Bill Karwin - author of IBPerl, Tim Bunce - author of DBI.

=head1 AUTHOR

Copyright (c) 1999 Edwin Pratomo <ed.pratomo@computer.org>.

All rights reserved. This is a B<free code>, available as-is;
you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

DBI(3), IBPerl(1).

=cut

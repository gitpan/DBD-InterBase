
#   $Id: InterBase.pm,v 1.18 2001/03/24 14:04:10 edpratomo Exp $
#
#   Copyright (c) 1999-2001 Edwin Pratomo
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.

require 5.003;

package DBD::InterBase;
use strict;
use Carp;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK $AUTOLOAD);
use DBI ();
require Exporter;
require DynaLoader;

@ISA = qw(Exporter DynaLoader);
$VERSION = '0.25.0';

bootstrap DBD::InterBase $VERSION;

use vars qw($VERSION $err $errstr $drh);

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

# taken from JWIED's DBD::mysql, with slight modification
sub _OdbcParse($$$) {
    my($class, $dsn, $hash, $args) = @_;
    my($var, $val);
    if (!defined($dsn)) {
        return;
    }
    while (length($dsn)) 
    {
        if ($dsn =~ /([^;]*)[;](.*)/) {
#        if ($dsn =~ /([^:;=]*)[:;](.*)/) {
            $val = $1;
            $dsn = $2;
        } else {
            $val = $dsn;
            $dsn = '';
        }
        if ($val =~ /([^=]*)=(.*)/) {
            $var = $1;
            $val = $2;
            if ($var eq 'hostname'  ||  $var eq 'host') 
            {
                $hash->{'host'} = $val;
            } elsif ($var eq 'db'  ||  $var eq 'dbname') 
            {
                $hash->{'database'} = $val;
            } else {
                $hash->{$var} = $val;
            }
        } else {
            foreach $var (@$args) {
                if (!defined($hash->{$var})) {
                    $hash->{$var} = $val;
                    last;
                }
            }
        }
    }
    $hash->{database} = "$hash->{host}:$hash->{database}" if $hash->{host};
}

sub _OdbcParseHost ($$) {
    my($class, $dsn) = @_;
    my($hash) = {};
    $class->_OdbcParse($dsn, $hash, ['host', 'port']);
    ($hash->{'host'}, $hash->{'port'});
}

package DBD::InterBase::dr;

sub connect {
    my($drh, $dsn, $dbuser, $dbpasswd, $attr) = @_;

    $dbuser ||= "SYSDBA";
    $dbpasswd ||= "masterkey";

    my ($this, $private_attr_hash);

    $private_attr_hash = {
        'Name' => $dsn,
        'user' => $dbuser,
        'password' => $dbpasswd
    };

    DBD::InterBase->_OdbcParse($dsn, $private_attr_hash,
                    ['database', 'host', 'port',
                     'ib_role', 'ib_charset', 'ib_dialect', 'ib_cache']);

    # second attr args will be retrieved using DBIc_IMP_DATA
    my $dbh = DBI::_new_dbh($drh, {}, $private_attr_hash);

    DBD::InterBase::db::_login($dbh, $dsn, $dbuser, $dbpasswd, $attr) 
        or return undef;

    $dbh;
}

package DBD::InterBase::db;
use strict;
use Carp;

sub do {
    my($dbh, $statement, $attr, @params) = @_;
    my $rows;
    if (@params) {
        my $sth = $dbh->prepare($statement, $attr) or return undef;
        $sth->execute(@params) or return undef;
        $rows = $sth->rows;
    } else {
        $rows = DBD::InterBase::db::_do($dbh, $statement, $attr)
            or return undef;
    }       
    ($rows == 0) ? "0E0" : $rows;
}

sub prepare {
    my ($dbh, $statement, $attribs) = @_;
    
    my $sth = DBI::_new_sth($dbh, {
        'Statement' => $statement });
    DBD::InterBase::st::_prepare($sth, $statement, $attribs)
        or return undef;
    $sth;
}

# from Christiaan Lademann <cal@zls.de> :

sub type_info_all {
    my $dbh = shift;
    my $names = {
        TYPE_NAME               => 0,
        DATA_TYPE               => 1,
        COLUMN_SIZE             => 2,
        LITERAL_PREFIX          => 3,
        LITERAL_SUFFIX          => 4,
        CREATE_PARAMS           => 5,
        NULLABLE                => 6,
        CASE_SENSITIVE          => 7,
        SEARCHABLE              => 8,
        UNSIGNED_ATTRIBUTE      => 9,
        FIXED_PREC_SCALE        =>10,
        AUTO_UNIQUE_VALUE       =>11,
        LOCAL_TYPE_NAME         =>12,
        MINIMUM_SCALE           =>13,
        MAXIMUM_SCALE           =>14,
        NUM_PREC_RADIX          =>15,
    };

    my $ti = [
        $names,
        # name type   prec prefix suffix  create params null case se unsign fix  auto
        # local    min    max  radix
        #                                            
        [ 'SQL_ARRAY', 0, 64536, undef, undef, undef, 1, '1', 0, undef, '0', '0',
'ARRAY', undef, undef, undef ],
        [ 'SQL_BLOB',  0, 64536, undef, undef, undef, 1, '1', 0, undef, '0', '0',
'BLOB', undef, undef, undef ],
        [ 'SQL_VARYING', DBI::SQL_VARCHAR, 32765,  '\'',  '\'', 'max length', 1, '1', 3, undef, '0', '0',
'VARCHAR', undef, undef, undef ],
        [ 'SQL_TEXT', DBI::SQL_VARCHAR, 32765,  '\'',  '\'', 'max length', 1, '1', 3, undef, '0', '0',
'TEXT', undef, undef, undef ],
        [ 'SQL_DOUBLE', DBI::SQL_DOUBLE, 18, undef, undef, 'precision', 1, '0', 2, '0', '0', '0',
'DOUBLE', undef, undef, undef ],
        [ 'SQL_QUAD', DBI::SQL_DOUBLE, 18, undef, undef, 'precision', 1, '0', 2, '0', '0', '0',
'QUAD', undef, undef, undef ],
        [ 'SQL_FLOAT', DBI::SQL_FLOAT, 12, undef, undef, 'precision', 1, '0', 2, '0', '0', '0',
'FLOAT', undef, undef, undef ],
        [ 'SQL_LONG', DBI::SQL_INTEGER, 10, undef, undef, undef, 1, '0', 2, '0', '0', '0',
'LONG', undef, undef, undef ],
        [ 'SQL_SHORT', DBI::SQL_SMALLINT, 5, undef, undef, undef, 1, '0', 2, '0', '0', '0',
'SHORT', undef, undef, undef ],
        [ 'SQL_TIMESTAMP', DBI::SQL_TIMESTAMP, 47, '\'', '\'', undef, 1, '0', 2, undef, '0', '0',
'TIMESTAMP', undef, undef, undef ],
        [ 'SQL_D_FLOAT', DBI::SQL_REAL, 24, undef, undef, 'precision', 1, '0', 2, '0', '0', '0',
'D_FLOAT', undef, undef, undef ],
        [ 'SQL_TYPE_TIME', DBI::SQL_TIME, 16, '\'', '\'', undef, 1, '0', 2, undef, '0', '0',
'TIME', undef, undef, undef ],
        [ 'SQL_TYPE_DATE', DBI::SQL_DATE, 10, '\'', '\'', undef, 1, '0', 2, undef, '0', '0',
'DATE', undef, undef, undef ],
        [ 'SQL_INT64', DBI::SQL_DOUBLE, 20, undef, undef, undef, 1, '0', 2, '0', '0', '0',
'INT64', undef, undef, undef ],

       ];
       return $ti;
}

# XXX TODO
#sub type_info
#{
#    my ($dbh, $type) = @_;
#    
#}

# from Michael Arnett <marnett@samc.com> :
sub tables
{
    my $dbh = shift;
    my @tables;
    my @row;
    my $stmt = 
'SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE (RDB$SYSTEM_FLAG IS NULL 
OR RDB$SYSTEM_FLAG = 0) AND RDB$VIEW_SOURCE IS NULL';

    my $sth = $dbh->prepare($stmt) or
        return undef;
    $sth->{ChopBlanks} = 1;
    $sth->execute;
    while (@row = $sth->fetchrow_array) {
        push(@tables, @row);
    }
    return @tables;
}

sub table_info
{
    my $dbh = shift;

    my $sth = $dbh->prepare(q{
      SELECT
        NULL                      TABLE_CAT, 
        a.rdb$owner_name          TABLE_SCHEM,
        a.rdb$relation_name       TABLE_NAME,
        CAST('TABLE' AS CHAR(5))  TABLE_TYPE,
        a.rdb$description         REMARKS
      FROM rdb$relations a
      WHERE a.rdb$system_flag=0 AND a.rdb$view_blr IS NULL
        UNION ALL
      SELECT
        NULL                      TABLE_CAT, 
        b.rdb$owner_name          TABLE_SCHEM,
        b.rdb$relation_name       TABLE_NAME,
        CAST('VIEW' AS CHAR(5))   TABLE_TYPE,
        b.rdb$description         REMARKS
      FROM rdb$relations b
      WHERE b.rdb$system_flag=0 AND b.rdb$view_blr IS NULL
    });
    $sth->execute() or return undef;

    return $sth;
}

sub ping {
    my($dbh) = @_;

    local $SIG{__WARN__} = sub { } if $dbh->{PrintError};
    local $dbh->{RaiseError} = 0 if $dbh->{RaiseError};
    my $ret = DBD::InterBase::db::_ping($dbh);

    return $ret;
}

1;

__END__

=head1 NAME

DBD::InterBase - DBI driver for InterBase RDBMS server

=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect("dbi:InterBase:db=$dbname", "sysdba", "masterkey");

  # See the DBI module documentation for full details

=head1 DESCRIPTION

DBD::InterBase is a Perl module which works with the DBI module to provide
access to InterBase databases.

=head1 MODULE DOCUMENTATION

This documentation describes driver specific behavior and restrictions. 
It is not supposed to be used as the only reference for the user. In any 
case consult the DBI documentation first !

=head1 THE DBI CLASS

=head2 DBI Class Methods

=over 4

=item B<connect>

To connect to a database with a minimum of parameters, use the 
following syntax: 

  $dbh = DBI->connect("dbi:InterBase:dbname=$dbname", "sysdba", "masterkey");

This connects to the database $dbname at localhost as SYSDBA user with the
default password. 

The following connect statement shows all possible parameters: 

 $dsn =
 "dbi:InterBase:dbname=$dbname;host=$host;ib_dialect=$dialect;ib_role=$role;ib_charset=$charset;ib_cache=$cache";
 $dbh =  DBI->connect($dsn, $username, $password);

The $dsn is prefixed by 'dbi:InterBase:', and consists of key-value
parameters separated by B<semicolons>. The following is the list of valid
parameters and their respective meanings:

    parameter   meaning                             optional?
    ---------------------------------------------------------
    database    path to the database                mandatory
    dbname      path to the database
    db          path to the database
    host        hostname (not IP address)           optional
    ib_dialect  the SQL dialect to be used          optional
    ib_role     the role of the user                optional
    ib_charset  character set to be used            optional
    ib_cache    number of database cache buffers    optional

B<database> could be used interchangebly with B<dbname> and B<db>. 
If a host is specified, connection will be made using TCP/IP sockets. 
For example, to connect to a Windows host, the $dsn will look like this:

 $dsn = "dbi:InterBase:db=C:/temp/test.gdb;host=rae.cumi.org;ib_dialect=3";

InterBase 6.0 introduces B<SQL dialect> to provide backward compatibility with
databases created by older versions of InterBase. In short, SQL dialect
controls how InterBase interprets:

 - double quotes
 - the DATE datatype
 - decimal and numeric datatypes
 - new 6.0 reserved keywords

Valid values for B<ib_dialect> are 1, 2, and 3. The driver's default value is
1. For more detail reading on this topic, please refer to:

 http://www.interbase.com/open/research/art_60dialects.html

B<ib_role> specifies the role of the connecting user. B<SQL role> is
implemented by InterBase to make database administration easier when dealing
with lots of users. A detailed reading can be found at:

 http://www.interbase.com/downloads/sqlroles.pdf

If B<ib_cache> is not specified, the default database's cache size value will be 
used. The InterBase Operations Guide discusses in full length the importance of 
this parameter to gain the best performance.

=item B<available_drivers>

  @driver_names = DBI->available_drivers;

Implemented by DBI, no driver-specific impact.

=item B<data_sources>

This method is not yet implemented.

=item B<trace>

  DBI->trace($trace_level, $trace_file)

Implemented by DBI, no driver-specific impact.

=back


=head2 DBI Dynamic Attributes

See Common Methods. 

=head1 METHODS COMMON TO ALL HANDLES

=over 4

=item B<err>

  $rv = $h->err;

Supported by the driver as proposed by DBI. 

=item B<errstr>

  $str = $h->errstr;

Supported by the driver as proposed by DBI. 

=item B<state>

This method is not yet implemented.

=item B<trace>

  $h->trace($trace_level, $trace_filename);

Implemented by DBI, no driver-specific impact.

=item B<trace_msg>

  $h->trace_msg($message_text);

Implemented by DBI, no driver-specific impact.

=item B<func>

See B<Transactions> section for information about invoking C<set_tx_param()>
from func() method.

=back

=head1 ATTRIBUTES COMMON TO ALL HANDLES

=over 4

=item B<Warn> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<Active> (boolean, read-only)

Supported by the driver as proposed by DBI. A database 
handle is active while it is connected and  statement 
handle is active until it is finished. 

=item B<Kids> (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<ActiveKids> (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<CachedKids> (hash ref)

Implemented by DBI, no driver-specific impact.

=item B<CompatMode> (boolean, inherited)

Not used by this driver. 

=item B<InactiveDestroy> (boolean)

Implemented by DBI, no driver-specific impact.

=item B<PrintError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<RaiseError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<ChopBlanks> (boolean, inherited)

Supported by the driver as proposed by DBI. 

=item B<LongReadLen> (integer, inherited)

Supported by the driver as proposed by DBI.The default value is 80 bytes. 

=item B<LongTruncOk> (boolean, inherited)

Supported by the driver as proposed by DBI.

=item B<Taint> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=back

=head1 DATABASE HANDLE OBJECTS

=head2 Database Handle Methods

=over 4

=item B<selectrow_array>

  @row_ary = $dbh->selectrow_array($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<selectall_arrayref>

  $ary_ref = $dbh->selectall_arrayref($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<selectcol_arrayref>

  $ary_ref = $dbh->selectcol_arrayref($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<prepare>

  $sth = $dbh->prepare($statement, \%attr);

Supported by the driver as proposed by DBI.
When AutoCommit is On, this method implicitly starts a new transaction,
which will be automatically committed after the following execute() or the
last fetch(), depending on the statement type. For select statements,
commit automatically takes place after the last fetch(), or by explicitly 
calling finish() method if there are any rows remaining. For non-select
statements, execute() will implicitly commits the transaction. 

=item B<prepare_cached>

  $sth = $dbh->prepare_cached($statement, \%attr);

Implemented by DBI, no driver-specific impact. 

=item B<do>

  $rv  = $dbh->do($statement, \%attr, @bind_values);

Supported by the driver as proposed by DBI.
This should be used for non-select statements, where the driver doesn't take
the conservative prepare - execute steps, thereby speeding up the execution
time. But if this method is used with bind values, the speed advantage
diminishes as this method calls prepare() for binding the placeholders.
Instead of calling this method repeatedly with bind values, it would be
better to call prepare() once, and execute() many times.

See the notes for the execute method elsewhere in this document. 

=item B<commit>

  $rc  = $dbh->commit;

Supported by the driver as proposed by DBI. See also the 
notes about B<Transactions> elsewhere in this document. 

=item B<rollback>

  $rc  = $dbh->rollback;

Supported by the driver as proposed by DBI. See also the 
notes about B<Transactions> elsewhere in this document. 

=item B<disconnect>

  $rc  = $dbh->disconnect;

Supported by the driver as proposed by DBI. 

=item B<ping>

  $rc = $dbh->ping;

This driver supports the ping-method, which can be used to check the 
validity of a database-handle. This is especially required by
C<Apache::DBI>.

=item B<table_info>

  $sth = $dbh->table_info;

Supported by the driver as proposed by DBI. 

=item B<tables>

  @names = $dbh->tables;

Supported by the driver as proposed by DBI. 

=item B<type_info_all>

  $type_info_all = $dbh->type_info_all;

Supported by the driver as proposed by DBI. 

For further details concerning the InterBase specific data-types 
please read the L<InterBase Data Definition Guide>. 

=item B<type_info>

  @type_info = $dbh->type_info($data_type);

Implemented by DBI, no driver-specific impact. 

=item B<quote>

  $sql = $dbh->quote($value, $data_type);

Implemented by DBI, no driver-specific impact. 

=back

=head2 Database Handle Attributes

=over 4

=item B<AutoCommit>  (boolean)

Supported by the driver as proposed by DBI. According to the 
classification of DBI, InterBase is a database, in which a 
transaction must be explicitly started. Without starting a 
transaction, every change to the database becomes immediately 
permanent. The default of AutoCommit is on, which corresponds 
to the DBI's default. When setting AutoCommit to off, a transaction 
will be started and every commit or rollback 
will automatically start a new transaction. For details see the 
notes about B<Transactions> elsewhere in this document. 

=item B<Driver>  (handle)

Implemented by DBI, no driver-specific impact. 

=item B<Name>  (string, read-only)

Not yet implemented.

=item B<RowCacheSize>  (integer)

Implemented by DBI, not used by the driver.

=back

=head1 STATEMENT HANDLE OBJECTS


=head2 Statement Handle Methods

=over 4

=item B<bind_param>

Supported by the driver as proposed by DBI. 
The SQL data type passed as the third argument is ignored. 

=item B<bind_param_inout>

Not supported by this driver. 

=item B<execute>

  $rv = $sth->execute(@bind_values);

Supported by the driver as proposed by DBI. 
On success, this method returns -1 instead of the number of affected rows.

=item B<fetchrow_arrayref>

  $ary_ref = $sth->fetchrow_arrayref;

Supported by the driver as proposed by DBI. 

=item B<fetchrow_array>

  @ary = $sth->fetchrow_array;

Supported by the driver as proposed by DBI. 

=item B<fetchrow_hashref>

  $hash_ref = $sth->fetchrow_hashref;

Supported by the driver as proposed by DBI. 

=item B<fetchall_arrayref>

  $tbl_ary_ref = $sth->fetchall_arrayref;

Implemented by DBI, no driver-specific impact. 

=item B<finish>

  $rc = $sth->finish;

Supported by the driver as proposed by DBI. 

=item B<rows>

  $rv = $sth->rows;

Supported by the driver as proposed by DBI. 
It returns the number of B<fetched> rows for select statements, otherwise
it returns -1 (unknown number of affected rows).

=item B<bind_col>

  $rc = $sth->bind_col($column_number, \$var_to_bind, \%attr);

Supported by the driver as proposed by DBI. 

=item B<bind_columns>

  $rc = $sth->bind_columns(\%attr, @list_of_refs_to_vars_to_bind);

Supported by the driver as proposed by DBI. 

=item B<dump_results>

  $rows = $sth->dump_results($maxlen, $lsep, $fsep, $fh);

Implemented by DBI, no driver-specific impact. 

=back

=head2 Statement Handle Attributes

=over 4

=item B<NUM_OF_FIELDS>  (integer, read-only)

Implemented by DBI, no driver-specific impact. 

=item B<NUM_OF_PARAMS>  (integer, read-only)

Implemented by DBI, no driver-specific impact. 

=item B<NAME>  (array-ref, read-only)

Supported by the driver as proposed by DBI. 

=item B<NAME_lc>  (array-ref, read-only)

Implemented by DBI, no driver-specific impact. 

=item B<NAME_uc>  (array-ref, read-only)

Implemented by DBI, no driver-specific impact. 

=item B<TYPE>  (array-ref, read-only)

Supported by the driver as proposed by DBI, with 
the restriction, that the types are InterBase
specific data-types which do not correspond to 
international standards.

=item B<PRECISION>  (array-ref, read-only)

Supported by the driver as proposed by DBI. 

=item B<SCALE>  (array-ref, read-only)

Supported by the driver as proposed by DBI. 

=item B<NULLABLE>  (array-ref, read-only)

Supported by the driver as proposed by DBI. 

=item B<CursorName>  (string, read-only)

Supported by the driver as proposed by DBI. 

=item B<Statement>  (string, read-only)

Supported by the driver as proposed by DBI. 

=item B<RowCache>  (integer, read-only)

Not supported by the driver. 

=back

=head1 FURTHER INFORMATION

=head2 Transactions

The transaction behavior is controlled with the attribute AutoCommit. 
For a complete definition of AutoCommit please refer to the DBI documentation. 

According to the DBI specification the default for AutoCommit is TRUE. 
In this mode, any change to the database becomes valid immediately. Any 
commit() or rollback() will be rejected. 

If AutoCommit is switched-off, immediately a transaction will be started.
A rollback() will rollback and close the active transaction, then implicitly 
start a new transaction. A disconnect will issue a rollback. 

InterBase provides fine control over transaction behavior, where users can
specify the access mode, the isolation level, the lock resolution, and the 
table reservation (for a specified table). For this purpose,
C<set_tx_param()> database handle method is available. Please notice that
this private method is new and considered B<experimental> at this release.
Use with caution!

Upon a successful C<connect()>, these default parameter values will be used
for every SQL operation:

    Access mode:        read/write
    Isolation level:    concurrency
    Lock resolution:    wait

Any of the above value can be changed using C<set_tx_param()>.

=over 4

=item B<set_tx_param> 

 $dbh->func( 
	-access_mode     => 'read_write',
	-isolation_level => 'read_committed',
	-lock_resolution => 'wait',
	'set_tx_param'
 );

Valid value for C<-access_mode> is C<read_write>, or C<read_only>. 
Valid value for C<-lock_resolution> is C<wait>, or C<no_wait>.
C<-isolation_level> may be: C<read_committed>, C<snapshot>,
C<snapshot_table_stability>. If C<read_committed> is to be used with
C<record_version> or C<no_record_version>, then they should be inside an
anonymous array:

 $dbh->func( 
	-isolation_level => ['read_committed', 'record_version']
    'set_tx_param'
 );

Currently, table reservation is not yet supported.

=back

=head2 Unsupported SQL Statements

Here is a list of SQL statements which can't be used. But this shouldn't be a 
problem, because their functionality are already provided by the DBI methods.

=over 4

=item * SET TRANSACTION

Use C<$dbh->func(..., 'set_tx_param')> instead.

=item * DESCRIBE

Provides information about columns that are retrieved by a DSQL statement,
or about placeholders in a statement. This functionality is supported by the
driver, and transparent for users. Column names are available via
$sth->{NAME} attributes.

=item * EXECUTE IMMEDIATE

Calling do() method without bind value(s) will do the same.

=item * CLOSE, OPEN, DECLARE CURSOR

$sth->{CursorName} is automagically available upon executing a "SELECT .. FOR
UPDATE" statement. A cursor is closed after the last fetch(), or by calling
$sth->finish(). 

=item * PREPARE, EXECUTE, FETCH

Similar functionalities are obtained by using prepare(), execute(), and 
fetch() methods.

=back

=head2 Compatibility with DBI Extension modules 

C<DBD::InterBase> is known to work with C<DBIx::Recordset> 0.21, and
C<Apache::DBI> 0.87. DBI 1.14 has a subtle bug on C<fetchall_arrayref> method,
when it is passed an empty hash ref. The patch is included here as 
DBI.pm.diff, because it is required to make the driver work with 
C<DBIx::Tree> 0.91. Yuri Vasiliev <I<yuri.vasiliev@targuscom.com>> reported 
successful usage with Apache::AuthDBI (part of C<Apache::DBI> 0.87 
distribution).

The driver is untested with C<Apache::Session::DBI>. Doesn't work with 
C<Tie::DBI>. C<Tie::DBI> calls $dbh->prepare("LISTFIELDS $table_name") on 
which InterBase fails to parse. I think that the call should be made within 
an eval block.

=head2 Tested Platforms

Client: Linux, glibc-2.1.2, x86 egcs-1.1.2, kernel 2.2.12-20. 
Server: InterBase 6.0 SS and Classic for Linux, InterBase 6.0 for Windows.

=head1 AUTHOR

=item * DBI by Tim Bunce <Tim.Bunce@ig.co.uk>

=item * DBD::InterBase by Edwin Pratomo <ed.pratomo@computer.org>

Partially based on the work of Bill Karwin's IBPerl, Jochen Wiedmann's
DBD::mysql, and Edmund Mergl's DBD::Pg.

=head1 BUGS

Known problem with BLOB fields on Win32 platform.

=head1 SEE ALSO

DBI(3).

=head1 COPYRIGHT

The DBD::InterBase module is a free software. 
You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file,
with the exception that it cannot be placed on a CD-ROM or similar media
for commercial distribution without the prior approval of the author.

=head1 ACKNOWLEDGEMENTS

Mark D. Anderson <I<mda@discerning.com>>, and Michael Samanov
<I<samanov@yahoo.com>> gave important feedbacks and ideas during the early
development days of this XS version. 

Michael Arnett <I<marnett@mediaone.net>>, Flemming Frandsen <I<dion@swamp.dk>>,
Mike Shoyher <I<msh@e-labs.ru>>, Christiaan Lademann <I<cal@zls.de>> sent me 
patches for the first version. Their code are still used in the current
version, with or without some modification.

=cut

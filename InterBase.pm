#   $Id: InterBase.pm,v 1.49 2004/02/25 04:38:03 edpratomo Exp $
#
#   Copyright (c) 1999-2004 Edwin Pratomo
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.

require 5.004;

package DBD::InterBase;
use strict;
use Carp;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK $AUTOLOAD);
use DBI ();
require Exporter;
require DynaLoader;

@ISA = qw(Exporter DynaLoader);
$VERSION = '0.43';

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

    $drh = DBI::_new_drh($class, {'Name' => 'InterBase',
                                  'Version' => $VERSION,
                                  'Err'    => \$DBD::InterBase::err,
                                  'Errstr' => \$DBD::InterBase::errstr,
                                  'Attribution' => 'DBD::InterBase by Edwin Pratomo and Daniel Ritz'});
    $drh;
}

# taken from JWIED's DBD::mysql, with slight modification
sub _OdbcParse($$$) 
{
    my($class, $dsn, $hash, $args) = @_;
    my($var, $val);

    if (!defined($dsn))
       { return; }

    while (length($dsn)) 
    {
        if ($dsn =~ /([^;]*)[;]\r?\n?(.*)/s) 
        {
            $val = $1;
            $dsn = $2;
        } 
        else 
        {
            $val = $dsn;
            $dsn = '';
        }
        if ($val =~ /([^=]*)=(.*)/) 
        {
            $var = $1;
            $val = $2;
            if ($var eq 'hostname') 
                { $hash->{'host'} = $val; } 
            elsif ($var eq 'db'  ||  $var eq 'dbname') 
                { $hash->{'database'} = $val; } 
            else 
                { $hash->{$var} = $val; }
        } 
        else 
        {
            foreach $var (@$args) 
            {
                if (!defined($hash->{$var})) 
                {
                    $hash->{$var} = $val;
                    last;
                }
            }
        }
    }
    $hash->{host} = "$hash->{host}/$hash->{port}" if ($hash->{host} && $hash->{port});
    $hash->{database} = "$hash->{host}:$hash->{database}" if $hash->{host};
}


package DBD::InterBase::dr;

sub connect 
{
    my($drh, $dsn, $dbuser, $dbpasswd, $attr) = @_;

    $dbuser   ||= "SYSDBA";
    $dbpasswd ||= "masterkey";

    my ($this, $private_attr_hash);

    $private_attr_hash = {
        'Name' => $dsn,
        'user' => $dbuser,
        'password' => $dbpasswd
    };

    DBD::InterBase->_OdbcParse($dsn, $private_attr_hash,
                               ['database', 'host', 'port', 'ib_role', 
                                'ib_charset', 'ib_dialect', 'ib_cache', 'ib_lc_time']);

    # second attr args will be retrieved using DBIc_IMP_DATA
    my $dbh = DBI::_new_dbh($drh, {}, $private_attr_hash);

    DBD::InterBase::db::_login($dbh, $dsn, $dbuser, $dbpasswd, $attr) 
        or return undef;

    $dbh;
}

package DBD::InterBase::db;
use strict;
use Carp;

sub do 
{
    my($dbh, $statement, $attr, @params) = @_;
    my $rows;
    if (@params) 
    {
        my $sth = $dbh->prepare($statement, $attr) or return undef;
        $sth->execute(@params) or return undef;
        $rows = $sth->rows;
    } 
    else 
    {
        $rows = DBD::InterBase::db::_do($dbh, $statement, $attr) or return undef;
    }       
    ($rows == 0) ? "0E0" : $rows;
}


sub prepare 
{
    my ($dbh, $statement, $attribs) = @_;
    
    my $sth = DBI::_new_sth($dbh, {'Statement' => $statement });
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
        SQL_DATA_TYPE           =>15,
        SQL_DATETIME_SUB        =>16,       
        NUM_PREC_RADIX          =>17,
        INTERVAL_PRECISION      =>18,
    };

    my $ti = [
        $names,
        # type-name            data-type           size   prefix suffix  create-params      null case srch, unsg   fix  auto local               min    max    sql-data-type       sql-datetime-sub num-prec-radix  int-prec
        [ 'BLOB',              0,                  64536, undef, undef,  undef,             1,   1,   0,    undef, 0,   0,   'BLOB',             undef, undef, 0,                  undef,           undef,          undef ],
        [ 'CHAR',              DBI::SQL_CHAR,      32765, '\'',  '\'',   undef,             1,   1,   3,    undef, 0,   0,   'CHAR',             undef, undef, DBI::SQL_CHAR,      undef,           undef,          undef ],
        [ 'CHARACTER',         DBI::SQL_CHAR,      32765, '\'',  '\'',   undef,             1,   1,   3,    undef, 0,   0,   'CHAR',             undef, undef, DBI::SQL_CHAR,      undef,           undef,          undef ],
        [ 'DATE',              DBI::SQL_DATE,      10,    '\'',  '\'',   undef,             1,   0,   2,    undef, 0,   0,   'DATE',             undef, undef, DBI::SQL_DATE,      undef,           undef,          undef ],
        [ 'DECIMAL',           DBI::SQL_DECIMAL,   20,    undef, undef,  'precision,scale', 1,   0,   2,    0,     0,   0,   'DECIMAL',          0,     18,    DBI::SQL_DECIMAL,   undef,           10,             undef ],
        [ 'DOUBLE PRECISION',  DBI::SQL_DOUBLE,    64,    undef, undef,  undef,             1,   0,   2,    0,     0,   0,   'DOUBLE PRECISION', undef, undef, DBI::SQL_DOUBLE,    undef,           2,              undef ],
        [ 'FLOAT',             DBI::SQL_FLOAT,     32,    undef, undef,  undef,             1,   0,   2,    0,     0,   0,   'FLOAT',            undef, undef, DBI::SQL_FLOAT,     undef,           2,              undef ],
        [ 'INTEGER',           DBI::SQL_INTEGER,   32,    undef, undef,  undef,             1,   0,   2,    0,     1,   0,   'INTEGER',          0,     0,     DBI::SQL_INTEGER,   undef,           2,              undef ],
        [ 'NUMERIC',           DBI::SQL_NUMERIC,   20,    undef, undef,  'precision,scale', 1,   0,   2,    0,     0,   0,   'NUMERIC',          0,     18,    DBI::SQL_NUMERIC,   undef,           10,             undef ],
        [ 'SMALLINT',          DBI::SQL_SMALLINT,  16,    undef, undef,  undef,             1,   0,   2,    undef, 1,   0,   'SMALLINT',         0,     0,     DBI::SQL_SMALLINT,  undef,           2,              undef ],
        [ 'TIME',              DBI::SQL_TIME,      64,    '\'',  '\'',   undef,             1,   0,   2,    undef, 1,   0,   'TIME',             4,     4,     DBI::SQL_TIME,      undef,           2,              undef ],
        [ 'TIMESTAMP',         DBI::SQL_TIMESTAMP, 64,    '\'',  '\'',   undef,             1,   0,   2,    undef, 1,   0,   'TIMESTAMP',        4,     4,     DBI::SQL_TIMESTAMP, undef,           2,              undef ],
        [ 'VARCHAR',           DBI::SQL_VARCHAR,   32765, '\'',  '\'',   'length',          1,   1,   3,    undef, 0,   0,   'VARCHAR',          undef, undef, DBI::SQL_VARCHAR,   undef,           undef,          undef ],
        [ 'CHAR VARYING',      DBI::SQL_VARCHAR,   32765, '\'',  '\'',   'length',          1,   1,   3,    undef, 0,   0,   'VARCHAR',          undef, undef, DBI::SQL_VARCHAR,   undef,           undef,          undef ],
        [ 'CHARACTER VARYING', DBI::SQL_VARCHAR,   32765, '\'',  '\'',   'length',          1,   1,   3,    undef, 0,   0,   'VARCHAR',          undef, undef, DBI::SQL_VARCHAR,   undef,           undef,          undef ],
       ];
       return $ti;
}


# from Michael Arnett <marnett@samc.com> :
sub tables
{
    my $dbh = shift;
    my @tables;
    my @row;

    my $sth = $dbh->prepare(q{
      SELECT rdb$relation_name 
      FROM rdb$relations 
      WHERE (rdb$system_flag IS NULL OR rdb$system_flag = 0) 
        AND rdb$view_source IS NULL;  
    }) or return undef;

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
      WHERE b.rdb$system_flag=0 AND b.rdb$view_blr IS NOT NULL
    });
    $sth->execute() or return undef;

    return $sth;
}

sub ping 
{
    my($dbh) = @_;

    local $SIG{__WARN__} = sub { } if $dbh->{PrintError};
    local $dbh->{RaiseError} = 0 if $dbh->{RaiseError};
    my $ret = DBD::InterBase::db::_ping($dbh);

    return $ret;
}

sub get_info {
    my ($dbh, $info_type) = @_;
    require DBD::InterBase::GetInfo;
    my $v = $DBD::InterBase::GetInfo::info{int($info_type)};
    $v = $v->($dbh) if ref $v eq 'CODE';
    return $v;
} 

1;

__END__

=head1 NAME

DBD::InterBase - DBI driver for Firebird and InterBase RDBMS server

=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect("dbi:InterBase:db=$dbname", "sysdba", "masterkey");

  # See the DBI module documentation for full details

=head1 DESCRIPTION

DBD::InterBase is a Perl module which works with the DBI module to provide
access to Firebird and InterBase databases.

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

Multiline DSN is acceptable. Here is an example of connect statement which uses all 
possible parameters: 

   $dsn =<< "DSN";
 dbi:InterBase:dbname=$dbname;
 host=$host;
 port=$port;
 ib_dialect=$dialect;
 ib_role=$role;
 ib_charset=$charset;
 ib_cache=$cache
 DSN

 $dbh =  DBI->connect($dsn, $username, $password);

The $dsn is prefixed by 'dbi:InterBase:', and consists of key-value
parameters separated by B<semicolons>. New line may be added after the
semicolon. The following is the list of valid parameters and their
respective meanings:

    parameter   meaning                             optional?
    ---------------------------------------------------------
    database    path to the database                required
    dbname      path to the database
    db          path to the database
    host        hostname (not IP address)           optional
    port        port number                         optional
    ib_dialect  the SQL dialect to be used          optional
    ib_role     the role of the user                optional
    ib_charset  character set to be used            optional
    ib_cache    number of database cache buffers    optional

B<database> could be used interchangebly with B<dbname> and B<db>. 
To connect to a remote host, use the B<host> parameter. 
Here is an example of DSN to connect to a remote Windows host:

 $dsn = "dbi:InterBase:db=C:/temp/test.gdb;host=rae.cumi.org;ib_dialect=3";

Firebird as of version 1.0 listens on port specified within the services
file. To connect to port other than the default 3050, add the port number at
the end of host name, separated by a slash. Example:

 $dsn = 'dbi:InterBase:db=/data/test.gdb;host=localhost/3060';

InterBase 6.0 introduces B<SQL dialect> to provide backward compatibility with
databases created by older versions of InterBase. In short, SQL dialect
controls how InterBase interprets:

 - double quotes
 - the DATE datatype
 - decimal and numeric datatypes
 - new 6.0 reserved keywords

Valid values for B<ib_dialect> are 1, 2, and 3. The driver's default value is
1. 

B<ib_role> specifies the role of the connecting user. B<SQL role> is
implemented by InterBase to make database administration easier when dealing
with lots of users. A detailed reading can be found at:

 http://www.ibphoenix.com/ibp_sqlroles.html

If B<ib_cache> is not specified, the default database's cache size value will be 
used. The InterBase Operation Guide discusses in full length the importance of 
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

=head1 METHODS COMMON TO ALL DBI HANDLES

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

See B<Transactions> section for information about invoking C<ib_set_tx_param()>
from func() method.

=back

=head1 ATTRIBUTES COMMON TO ALL DBI HANDLES

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

See the notes for the execute method elsewhere in this document. Unlike the
execute method, currently this method doesn't return the number of affected
rows. 

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

=item B<ib_softcommit>  (driver-specific, boolean)

Set this attribute to TRUE to use InterBase's soft commit feature (default
to FALSE). Soft commit retains the internal transaction handle when
committing a transaction, while the default commit behavior always closes
and invalidates the transaction handle.

Since the transaction handle is still open, there is no need to start a new transaction 
upon every commit, so applications can gain performance improvement. Using soft commit is also 
desirable when dealing with nested statement handles under AutoCommit on. 

Switching the attribute's value from TRUE to FALSE will force hard commit thus 
closing the current transaction. 

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

=head1 DRIVER SPECIFIC INFORMATION

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
C<ib_set_tx_param()> database handle method is available. 

Upon a successful C<connect()>, these default parameter values will be used
for every SQL operation:

    Access mode:        read/write
    Isolation level:    concurrency
    Lock resolution:    wait

Any of the above value can be changed using C<ib_set_tx_param()>.

=over 4

=item B<ib_set_tx_param> 

 $dbh->func( 
    -access_mode     => 'read_write',
    -isolation_level => 'read_committed',
    -lock_resolution => 'wait',
    'ib_set_tx_param'
 );

Valid value for C<-access_mode> is C<read_write>, or C<read_only>. 
Valid value for C<-lock_resolution> is C<wait>, or C<no_wait>.
C<-isolation_level> may be: C<read_committed>, C<snapshot>,
C<snapshot_table_stability>. If C<read_committed> is to be used with
C<record_version> or C<no_record_version>, then they should be inside an
anonymous array:

 $dbh->func( 
    -isolation_level => ['read_committed', 'record_version'],
    'ib_set_tx_param'
 );

Table reservation is supported since C<DBD::InterBase 0.30>. Names of the
tables to reserve as well as their reservation params/values are specified
inside a hashref, which is then passed as the value of C<-reserving>.

The following example reserves C<foo_table> with C<read> lock and C<bar_table> 
with C<read> lock and C<protected> access:

 $dbh->func(
    -access_mode     => 'read_write',
    -isolation_level => 'read_committed',
    -lock_resolution => 'wait',
    -reserving       =>
        {
            foo_table => {
                lock    => 'read',
            },
            bar_table => {
                lock    => 'read',
                access  => 'protected',
            },
        },
    'ib_set_tx_param'
 );

Possible table reservation parameters are:

=over 4

=item C<access> (optional)

Valid values are C<shared> or C<protected>.

=item C<lock> (required)

Valid values are C<read> or C<write>.

=back

Under C<AutoCommit> mode, invoking this method doesn't only change the
transaction parameters (as with C<AutoCommit> off), but also commits the
current transaction. The new transaction parameters will be used in
any newly started transaction. 

C<ib_set_tx_param()> can also be invoked with no parameter in which it resets
transaction parameters to the default value.

=back

=head2 DATE, TIME, and TIMESTAMP Formats

C<DBD::InterBase> supports various formats for query results of DATE, TIME,
and TIMESTAMP types. 

By default, it uses "%c" for TIMESTAMP, "%x" for DATE, and "%X" for TIME,
and pass them to ANSI C's strftime() function to format your query results.
These values are respectively stored in ib_timestampformat, ib_dateformat,
and ib_timeformat attributes, and may be changed in two ways:

=over 

=item * At $dbh level

This replaces the default values. Example:

 $dbh->{ib_timestampformat} = '%m-%d-%Y %H:%M';
 $dbh->{ib_dateformat} = '%m-%d-%Y';
 $dbh->{ib_timeformat} = '%H:%M';

=item * At $sth level

This overrides the default values only for the currently prepared statement. Example:

 $attr = {
    ib_timestampformat => '%m-%d-%Y %H:%M',
    ib_dateformat => '%m-%d-%Y',
    ib_timeformat => '%H:%M',
 };
 # then, pass it to prepare() method. 
 $sth = $dbh->prepare($sql, $attr);

=back

Since locale settings affect the result of strftime(), if your application
is designed to be portable across different locales, you may consider using these
two special formats: 'TM' and 'ISO'. C<TM> returns a 9-element list, much like
Perl's localtime(). The C<ISO> format applies sprintf()'s pattern
"%04d-%02d-%02d %02d:%02d:%02d.%04d" for TIMESTAMP, "%04d-%02d-%02d" for
DATE, and "%02d:%02d:%02d.%04d" for TIME. 

C<$dbh-E<gt>{ib_time_all}> can be used to specify all of the three formats at
once. Example:

 $dbh->{ib_time_all} = 'TM';


=head2 Using Event Alerter

This new feature is experimental and subjects to change. 

=over

=item C<ib_init_event>

 $evh = $dbh->func(@event_names, 'ib_init_event');

Initialize an event handle from several event names.

=item C<ib_wait_event>

 $dbh->func($evh, 'ib_wait_event');

Wait synchronously for particular events registered via event handle $evh.

=item C<ib_register_callback>

 $dbh->func($evh, sub { print "callback..\n" }, 'ib_register_callback');

Register a callback for asynchronous wait.

=item C<ib_reinit_event>

 $dbh->func($evh, 'ib_reinit_event');

Reinitialize event handle.

=back

=head2 Retrieving Firebird/InterBase specific information

=over

=item C<ib_database_info>

 $hash_ref = $dbh->func(@info, 'ib_database_info');
 $hash_ref = $dbh->func([@info], 'ib_database_info');

Retrieve database information from current connection. 

=item C<ib_plan>

 $plan = $sth->func('ib_plan');

Retrieve query plan from a prepared SQL statement. 

 my $sth = $dbh->prepare('SELECT * FROM foo');
 print $sth->func('ib_plan'); # PLAN (FOO NATURAL)

=back

=head2 Obsolete Features

=over 

=item Private Method

C<set_tx_param()> is obsoleted by C<ib_set_tx_param()>.

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
C<Apache::DBI> 0.87. Yuri Vasiliev <I<yuri.vasiliev@targuscom.com>> reported 
successful usage with Apache::AuthDBI (part of C<Apache::DBI> 0.87 
distribution).

The driver is untested with C<Apache::Session::DBI>. Doesn't work with 
C<Tie::DBI>. C<Tie::DBI> calls $dbh->prepare("LISTFIELDS $table_name") on 
which InterBase fails to parse. I think that the call should be made within 
an eval block.

=head1 TESTED PLATFORMS

=head2 Client

=over 4

=item Linux

=item FreeBSD

=item SPARC Solaris

=item Win32

=back

=head2 Server

=over 4

=item InterBase 6.0/6.01 SS and Classic for Linux

=item InterBase 6.0/6.01 for Windows, FreeBSD, SPARC Solaris

=item Firebird 1.0 Final SS for Windows, Linux, SPARC Solaris

=item Firebird 1.5 RC7 for Windows, Linux

=item Firebird 1.5 Final for Linux

=back

=head1 AUTHORS

=over 4

=item * DBI by Tim Bunce <Tim.Bunce@pobox.com>

=item * DBD::InterBase by Edwin Pratomo <edpratomo@cpan.org> and Daniel Ritz 
<daniel.ritz@gmx.ch>.

This module is originally based on the work of Bill Karwin's IBPerl.

=back

=head1 BUGS/LIMITATIONS

No bugs known at this time. But there are some limitations:

=over 4

=item * Arrays are not (yet) supported

=item * Read/Write BLOB fields block by block not (yet) supported. The
maximum size of a BLOB read/write is hardcoded to about 1MB.

=back

=head1 SEE ALSO

DBI(3).

=head1 COPYRIGHT

The DBD::InterBase module is Copyright (c) 1999-2004 Edwin Pratomo.
Portions Copyright (c) 2001-2003  Daniel Ritz.

The DBD::InterBase module is free software. 
You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file,
with the exception that it cannot be placed on a CD-ROM or similar media
for commercial distribution without the prior approval of the author.

=head1 ACKNOWLEDGEMENTS

An attempt to enumerate all who have contributed patches (may misses some):
Igor Klingen, Sergey Skvortsov, Ilya Verlinsky, Pavel Zheltouhov, Peter
Wilkinson, Mark D. Anderson, Michael Samanov, Michael Arnett, Flemming
Frandsen, Mike Shoyher, Christiaan Lademann. 

=cut

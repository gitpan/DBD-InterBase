
#   $Id: Makefile.PL,v 1.12 2001/05/13 22:25:46 danielritz Exp $
#
#   Copyright (c) 1999-2001 Edwin Pratomo
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.

BEGIN { $^W = 0 }       # turn warnings off
BEGIN { require 5.003 } # 5.003 fixes very important bugs

use ExtUtils::MakeMaker 5.16, qw(prompt &WriteMakefile $Verbose);
use Config;
use Carp;
use strict;
use File::Basename;
use vars qw($Registry);


# This DBI must be installed before we can build a DBD.
# For those not using Dynamic loading this means building a
# new static perl in the DBI directory by saying 'make perl'
# and then using _that_ perl to make this one.
use DBI 1.08;
use DBI::DBD;   # DBD creation tools

my $dbi_arch_dir = dbd_dbi_arch_dir();
my $ib_dir_prefix;

# init stuff
my $IB_Bin_path = '';
my $isql_name;
my @ib_bin_dirs;
my @ib_inc_dirs;
my $ib_lib_dir = '';

################################################################################
# OS specific configuration
################################################################################
if ($Config::Config{osname} eq 'MSWin32')
{
  $isql_name = 'isql.exe';
 
  # try to find InterBase installation via the registry
  my $ib_bin_dir = '';
  my $ib_inc_dir = '';
  eval 
  {
    require Win32::TieRegistry;
	Win32::TieRegistry->import('$Registry');
	$Registry->Delimiter("/");

    my $sw = $Registry->{"LMachine/Software/"};

    # We have to check more than one keys, because different
    # releases of InterBase have used different key hierarchies.
    my $key = $sw->{"InterBase Corp/InterBase/CurrentVersion/"} ||
	          $sw->{"Borland/InterBase/CurrentVersion/"};

    if (defined($key)) 
    { 
      $ib_bin_dir = $key->{"/ServerDirectory"}; 
      $ib_inc_dir = $key->{"/RootDirectory"} . "SDK\\include"; 
      $ib_lib_dir = $key->{"/RootDirectory"} . "SDK\\";
    }
  };
  @ib_bin_dirs = ($ib_bin_dir);
  @ib_inc_dirs = ($ib_inc_dir);
}
else
{
  $isql_name = 'isql';
  @ib_bin_dirs = (qw(/usr/interbase/bin /opt/interbase/bin /usr/bin /usr/local/bin));
  @ib_inc_dirs = (qw(/usr/interbase/include /opt/interbase/include /usr/include));
}

################################################################################
# sub test_files - checks if at least one of the files in the list exists
# Paramters:
#  0: directory
#  1: reference to file list 
# Return value: true value if at least on file exists, 0 otherwise
################################################################################
sub test_files
{
  my($dir, $files) = @_;
  local $_;
  -f "$dir/$_" && return $_ for @$files;
  0;
}

################################################################################
# sub dir_choice - promts for a directory
# Paramters: 
#  0: prompt string
#  1: reference to direcotry list
#  2: reference to file list
# Return value: directory name
################################################################################
sub dir_choice 
{
  my($prompt, $dirs, $files) = @_;
  my %dirs = ('a' => 'other');
  my $i;
  my $ret;

  test_files($_, $files) && ($dirs{++$i} = $_) for @$dirs;
  while() 
  {
    print "\n$prompt\n\n";
    print "$_) $dirs{$_}\n" for sort keys %dirs;

    my $choice = prompt("Your choice:");
    next unless $dirs{$choice};

    $ret = ($choice eq 'a') ? prompt("Directory:", $ret) : $dirs{$choice};
    return $ret if test_files($ret, $files);
    print "\nDirectory is bad\n";
  }
}

################################################################################
# sub make_test_conf - configure for test (save to ./t/test.conf)
# Parameters: <none>
# Return value: <none>
################################################################################
sub make_test_conf 
{
  my $test_conf = './t/test.conf';
  my ($dsn, $user, $pass, $path);

  # read cached config if available
  if (-r $test_conf) 
  {
    print "Reading cached test configuration...\n";
    open F, $test_conf or die "Can't open $test_conf: $!";
    local @ARGV = ($test_conf);
    ($dsn, $user, $pass) = map {chomp;$_} <>;
    ($path) = $dsn =~ /(?!db|database)=([^;]+);/;
    close F;
  }

  # ask for database path
  for (1..3) 
  {
    $path = prompt("\nFull path to your test database:", $path);
    last if $path;
  }

  die "Must specify a test database" unless $path;

  # if DB doesn't exist ask for creation
  unless (-f $path) 
  {
    print <<"EOM";
Can't find $path
Trying to create the test database..
Please enter a username with CREATE DATABASE permission.
EOM

    $user = prompt("Username:", $user);
    $pass = prompt("Password:", $pass);
    create_test_db($path, $user, $pass);
  } 
  else 
  {
    $user = prompt("Username:", $user);
    $pass = prompt("Password:", $pass);
  }

  # save test config to file
  open F, ">$test_conf" or die "Can't write $test_conf: $!";
  print F "dbi:InterBase:db=$path;ib_dialect=3\n$user\n$pass\n";
  close F;
}

################################################################################
# sub create_test_db - Creates the test database
# Parameters:
#  0: path to testdatabase to be created
#  1: username used to connect to the DB
#  2: password
# Return value: <none>
################################################################################
sub create_test_db 
{
  my ($path, $user, $pass) = @_;

  # create the SQL file with CREATE statement
  open(T, ">./t/create.sql") or die "Can't write to t/create.sql";
  while(<DATA>) 
  {
    s/__TESTDB__/$path/;
    s/__USER__/$user/;
    s/__PASS__/$pass/;
    print T;                
  }
  close T;

  # try to find isql
  my $isql;
  if (-x "$IB_Bin_path/$isql_name") 
     { $isql = "$IB_Bin_path/$isql_name"; }
  else
  {
    for (split /:/, $ENV{PATH}) 
    {
      s#/+$##g; 
      if (-x "$_/$isql_name") 
         { $isql = "$_/$isql_name"; last; }
    }
  }

  EXEC: 
  {
    for (1..3) 
    {
      $isql = prompt("Enter full path to isql: ", $isql);
      last EXEC if (-x $isql);
    }
    die "Unable to execute isql. Aborting..";
  }
  
  # if test db directory doesn't exist -> try to create
  my $dir = dirname $path;
  unless (-d $dir) 
  {
    print "Can't find $dir. Trying to mkdir..\n";
    system('mkdir', '-p', $dir) == 0 
      or die "Can't mkdir -p $dir";
  }

  # try to execute isql and create the test database
  system($isql, '-i', './t/create.sql') == 0 
    or die "Fail calling $isql -i t/create/sql: $?";
}

################################################################################
# MAIN
################################################################################

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

# prompt for InterBase bin directory
$IB_Bin_path = dir_choice("InterBase bin directory", [@ib_bin_dirs], [qw(gfix gfix.exe)]);

unless(-x $IB_Bin_path)
{
  carp "I cannot find your InterBase installation.\nDBD::InterBase cannot build or run without InterBase.\n";
  exit 1;
}

# get InterBase version
my $IBVERSION;
my $GFIX_PATH = $IB_Bin_path . "/" . test_files($IB_Bin_path, [qw(gfix gfix.exe)]);
chop($IBVERSION = `$GFIX_PATH -z 2>&1`);
$IBVERSION =~ s/^gfix version //o;
my $is_final = $IBVERSION =~ /\.6\d+$/ ? 1 : 0;

# prompt for IB include dir
my $ib_inc = dir_choice("Interbase include directory", [@ib_inc_dirs], [qw(gds.h ibase.h)]);


# we use a hash for the MakeMaker parameters
my %MakeParams = (
  'NAME'          => 'DBD::InterBase',
  'VERSION_FROM'  => 'InterBase.pm', # finds $VERSION
  'C'             => ['dbdimp.c'],
  'H'             => ['dbdimp.h', 'InterBase.h'],
  'CCFLAGS'       => '',
  'INC'           => "-I\"$ib_inc\" -I\"$dbi_arch_dir\"",
  'OBJECT'        => "InterBase.o dbdimp.o",
  'LIBS'          => [''],
  'OPTIMIZE'      => $Config::Config{'optimize'},
  'XSPROTOARG'    => '-noprototypes',
  'dist'          => {COMPRESS=>'gzip -9f', SUFFIX=>'gz'},
  'realclean'     => '*.xsi test.conf',
);


# the OS specific build environment setup
my $os = $Config::Config{'osname'};
if ($os eq 'MSWin32')
{
  # Why does software development on Windows
  # have to be so difficult!?
    
  if ($Config::Config{'cc'} eq "cl") 
  {
    # Microsoft Visual C++ compiler
    my $VC_PATH = dir_choice("MS VC++ directory",
      [qw(c:/Progra~1/MICROS~2.NET/Vc7 C:/Progra~1/DevStudio/Vc)],
      [qw(bin/cl.exe)]);
    unless(-x $VC_PATH)
    {
      carp "I can't find your MS VC++ installation.\nDBD::InterBase cannot build.\n";
      exit 1;
    }
    my $vc_inc = $VC_PATH . "/include"; 
    my $vc_lib = $VC_PATH . "/lib"; 

    $INC .= " -I\"$vc_inc\"";

    my $ib_lib = dir_choice("Interbase lib directory",
      [qw(c:/Progra~1/InterB~1/InterB~1/SDK/lib_ms 
          c:/Progra~1/Borland/InterB~1/SDK/lib_ms), 
          "$ib_lib_dir\\lib_ms"],
      [qw(gds32_ms.lib)]);

    my $cur_libs = $Config::Config{'libs'} ;
    my $cur_lddlflags = $Config::Config{'lddlflags'} ;
     
    eval "
    sub MY::const_loadlibs {
    '
LDLOADLIBS = $ib_lib/gds32_ms.lib $ib_lib/ib_util_ms.lib $cur_libs
LDDLFLAGS = /LIBPATH:$vc_lib $cur_lddlflags
    '
    } ";
  } 
  else 
  {
    # Borland C++ 5.5
    my $bcc = $Config::Config{'bcc_path'} . "\\";

    my $BCC_PATH = dir_choice("Borland C++ directory",
      [qw(c:/borland/bcc55), $bcc],
      [qw(bin/bcc32.exe)]);

    unless(-x $BCC_PATH)
    {
      carp "I can't find your Borland C++ installation.\nDBD::InterBase cannot build.\n";
      exit 1;
    }

    my $bcc_inc = $BCC_PATH . "include"; 
    $MakeParams{'CCFLAGS'}    = '-a4 -DWIN32 -DNO_STRICT';
    $MakeParams{'OPTIMIZE'}   = "-O2";
    $MakeParams{'INC'}       .= " -I\"$bcc_inc\"";

    eval "
	sub MY::const_loadlibs {
	'
LDLOADLIBS = \$(LIBC) import32.lib gds32.lib cw32.lib
LDDLFLAGS = -L\"$ib_lib_dir\\lib\" -L\"$BCC_PATH\\lib\"
	'
	}
	";
  }
  # Will I need Cygwin rules too?
}
elsif ($os eq 'solaris')
{
  $MakeParams{'LIBS'} = '-lgdsmt -lm -lc'; 
}
elsif ($os eq 'linux')
{
  # This is for both:
  # o Free IB 4.0 for Red Hat Linux 4.2
  # o Commercial IB 5.1.1 for Red Hat Linux 5.1
  my $ib_lib = dir_choice("Interbase lib directory",
    [qw(/usr/interbase/lib /opt/interbase/lib /usr/lib /usr/local/lib)],
    [qw(libgds.a libgds.so)]);

  $MakeParams{'LIBS'} = "-L$ib_lib -lgds -ldl ";

  # o Commercial IB 5.1.1 for Red Hat Linux 6.x
  #   requires a compatibilty library to resolve
  #   symbols such as _xstat that were removed
  #   from glibc 2.1.  IB 5.6 and later don't need
  #   this, but it doesn't hurt to list the library.
  if (-f ('/lib/libNoVersion-2.1.2.so'))
  {
     $MakeParams{'LIBS'} .= ' -lNoVersion-2.1.2'
  }
}
elsif ($os eq 'freebsd')
{
  my $ib_lib = dir_choice("Interbase lib directory",
    [qw(/usr/interbase/lib /usr/lib)],
    [qw(libgds.a libgds.so)]);
  $MakeParams{'LIBS'} = "-L$ib_lib -lgds";
}
elsif ($os eq 'hpux')
{
  $MakeParams{'LIBS'} = '-lgds -ldld';
}
elsif ($os eq 'sco')
{
  # Uncomment this line if you use InterBase 4.0 for SCO OSR5:
  # $LIBS = '-b elf -B dynamic -lgds -lsocket -lcrypt_i';

  # Uncomment this line if you use InterBase 5.5 for SCO OSR5:
  $MakeParams{'LIBS'} = '-lgds -lsocket -lcrypt_i -lc -lm';
}
elsif ($os eq 'sunos')
{
  $MakeParams{'LIBS'} = '-lgdslib -ldl';
}
elsif ($os eq 'irix')
{
  $MakeParams{'LIBS'} = '-lgds -lsun';
}
elsif ($os eq 'aix')
{
  $MakeParams{'LIBS'} = '-lgdsshr';
}
elsif ($os eq 'dgux')
{
  $MakeParams{'LIBS'} = '-lgds -lgdsf -ldl -ldgc';
}
elsif ($os eq 'osf1')
{
  $MakeParams{'LIBS'} = '-lgds';
}
elsif ($os eq 'sysv')
{
   $MakeParams{'LIBS'} = '-lgds -lnsl -lsocket -ldl';
}
else
{
  carp "DBD::InterBase is not supported on platform $os.\n";
  exit 1;
}

# create the test config file
make_test_conf();

# and last but not least write the makefile
WriteMakefile(%MakeParams);

package MY;

sub postamble
{
  &::dbd_postamble(@_);
}

package main;

# the data used to create the database creation script
__DATA__
CREATE DATABASE "__TESTDB__" user "__USER__" password "__PASS__";

quit;

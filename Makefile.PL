
#   $Id: Makefile.PL,v 1.11 2001/03/25 01:57:53 edpratomo Exp $
#
#   Copyright (c) 1999-2001 Edwin Pratomo
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.

BEGIN { $^W = 1 }
BEGIN { require 5.003 } # 5.003 fixes very important bugs

use ExtUtils::MakeMaker 5.16, qw(prompt &WriteMakefile $Verbose);
use Config;
use Carp;
use strict;
use File::Basename;

# This DBI must be installed before we can build a DBD.
# For those not using Dynamic loading this means building a
# new static perl in the DBI directory by saying 'make perl'
# and then using _that_ perl to make this one.
use DBI 1.08;
use DBI::DBD;   # DBD creation tools

my $dbi_arch_dir = dbd_dbi_arch_dir();
my $ib_dir_prefix;

sub test_files {
  my($dir, $files) = @_;
  local $_;
  -f "$dir/$_" && return $_ for @$files;
  0;
}

sub dir_choice {
  my($prompt, $dirs, $files) = @_;
  my %dirs = ('a' => 'other');
  my $i;
  test_files($_, $files) && ($dirs{++$i} = $_) for @$dirs;
  my $ret;
  while() {
    print "\n$prompt\n\n";
    print "$_) $dirs{$_}\n" for sort keys %dirs;
    my $choice = prompt("Your choice:");
    next unless $dirs{$choice};
    $ret = ($choice eq 'a') ? prompt("Directory:", $ret) : $dirs{$choice};
    return $ret if test_files($ret, $files);
    print "\nDirectory is bad\n";
  }
}

sub make_test_conf {
    my $test_conf = './t/test.conf';
    my ($dsn, $user, $pass, $path);
    if (-r $test_conf) {
        print "Reading cached test configuration...\n";
        open F, $test_conf or die "Can't open $test_conf: $!";
        local @ARGV = ($test_conf);
        ($dsn, $user, $pass) = map {chomp;$_} <>;
        ($path) = $dsn =~ /(?!db|database)=([^;]+);/;
        close F;
    }

    for (1..3) {
        $path = prompt("\nFull path to your test database:", $path);
        last if $path;
    }

    die "Must specify a test database" unless $path;

    unless (-f $path) {
        print <<"EOM";
Can't find $path
Trying to create the test database..
Please enter a username with CREATE DATABASE permission.
EOM

        $user = prompt("Username:", $user);
        $pass  = prompt("Password:", $pass);
        create_test_db($path, $user, $pass);
    } else {
        $user = prompt("Username:", $user);
        $pass  = prompt("Password:", $pass);
    }
    open F, ">$test_conf" or die "Can't write $test_conf: $!";
    print F "dbi:InterBase:db=$path;ib_dialect=3\n$user\n$pass\n";
    close F;
}

sub create_test_db {
    my ($path, $user, $pass) = @_;

    open T, ">./t/create.sql" or die "Can't write to t/create.sql";
    while(<DATA>) {
        s/__TESTDB__/$path/;
        s/__USER__/$user/;
        s/__PASS__/$pass/;
        print T;                
    }
    close T;

    my $isql;
    for (split /:/, $ENV{PATH}) {
        s#/+$##g; 
        if (-x "$_/isql") {
            $isql = "$_/isql";
            last;
        }
    }

EXEC: 
    {
        for (1..3) {
            $isql = prompt("Enter full path to isql: ", $isql);
            last EXEC if (-x $isql);
        }
        die "Unable to execute isql. Aborting..";
    }
    
    my $dir = dirname $path;
    unless (-d $dir) {
        print "Can't find $dir. Trying to mkdir..\n";
        system('mkdir', '-p', $dir) == 0 
            or die "Can't mkdir -p $dir";
    }
    system($isql, '-i', './t/create.sql') == 0 
        or die "Fail calling $isql -i t/create/sql: $?";
}

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

my $GFIX_PATH = dir_choice("InterBase bin directory",
  [qw(/usr/interbase/bin /opt/interbase/bin /usr/bin /usr/local/bin
    c:/Progra~1/InterB~1/InterB~1/bin c:/Progra~1/Borland/InterB~1/bin)],
  [qw(gfix gfix.exe)]);

unless(-x $GFIX_PATH)
{
  carp "I cannot find your InterBase installation.\nDBD::InterBase cannot build or run without InterBase.\n";
  exit 1;
}

my $IBVERSION;
$GFIX_PATH .= "/" . test_files($GFIX_PATH, [qw(gfix gfix.exe)]);
chop($IBVERSION = `$GFIX_PATH -z 2>&1`);
$IBVERSION =~ s/^gfix version //o;

my $is_final = $IBVERSION =~ /\.6\d+$/ ? 1 : 0;
my $ib_inc = dir_choice("Interbase include directory",
  [qw(/usr/interbase/include /opt/interbase/include /usr/include
    c:/Progra~1/InterB~1/InterB~1/SDK/include c:/Progra~1/Borland/InterB~1/SDK/include)],
  [qw(gds.h ibase.h)]);

my $INC = "-I$ib_inc -I$dbi_arch_dir";
my $LDDLFLAGS = '';
my $LIBS = '';
my $OPT = $Config::Config{'optimize'};

my $CCFLAGS = '';

my $os = $Config::Config{'osname'};

if ($os eq 'MSWin32')
{
    # Why does software development on Windows
    # have to be so difficult!?
    
    if ($Config::Config{'cc'} eq "cl") {
        # ms
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
          [qw(c:/Progra~1/InterB~1/InterB~1/SDK/lib_ms c:/Progra~1/Borland/InterB~1/SDK/lib_ms)],
          [qw(gds32_ms.lib)]);

        my $cur_libs = $Config::Config{'libs'} ;
        my $cur_lddlflags = $Config::Config{'lddlflags'} ;

        eval "
sub MY::const_loadlibs
{
'
LDLOADLIBS = $ib_lib/gds32_ms.lib $ib_lib/ib_util_ms.lib $cur_libs
LDDLFLAGS = /LIBPATH:$vc_lib $cur_lddlflags
'
}
        #";
    } else {
        # borland
        my $BCC_PATH = dir_choice("Borland C++ directory",
          [qw(c:/borland/bcc55)],
          [qw(bin/bcc32.exe)]);
        unless(-x $BCC_PATH)
        {
          carp "I can't find your Borland C++ installation.\nDBD::InterBase cannot build.\n";
          exit 1;
        }
        my $bcc_inc = $BCC_PATH . "/include"; 
        $CCFLAGS = '-a4';
        $OPT = "-O2";
        $INC .= " -I\"$bcc_inc\"";

    }
    # Will I need Cygwin rules too?
}
elsif ($os eq 'solaris')
{
    $LIBS = '-lgdsmt -lm -lc'; 
}
elsif ($os eq 'linux')
{
    # This is for both:
    # o Free IB 4.0 for Red Hat Linux 4.2
    # o Commercial IB 5.1.1 for Red Hat Linux 5.1
    my $ib_lib = dir_choice("Interbase lib directory",
      [qw(/usr/interbase/lib /opt/interbase/lib /usr/lib /usr/local/lib)],
      [qw(libgds.a libgds.so)]);

    $LIBS = "-L$ib_lib -lgds -ldl ";

    # o Commercial IB 5.1.1 for Red Hat Linux 6.x
    #   requires a compatibilty library to resolve
    #   symbols such as _xstat that were removed
    #   from glibc 2.1.  IB 5.6 and later don't need
    #   this, but it doesn't hurt to list the library.
    if (-f ('/lib/libNoVersion-2.1.2.so'))
    {
    $LIBS .= ' -lNoVersion-2.1.2'
    }
}
elsif ($os eq 'freebsd')
{
    my $ib_lib = dir_choice("Interbase lib directory",
      [qw(/usr/interbase/lib /usr/lib)],
      [qw(libgds.a libgds.so)]);
    $LIBS = "-L$ib_lib -lgds";
}
elsif ($os eq 'hpux')
{
    $LIBS = '-lgds -ldld';
}
elsif ($os eq 'sco')
{
    # Uncomment this line if you use InterBase 4.0 for SCO OSR5:
    # $LIBS = '-b elf -B dynamic -lgds -lsocket -lcrypt_i';

    # Uncomment this line if you use InterBase 5.5 for SCO OSR5:
    $LIBS = '-lgds -lsocket -lcrypt_i -lc -lm';
}
elsif ($os eq 'sunos')
{
    $LIBS = '-lgdslib -ldl';
}
elsif ($os eq 'irix')
{
    $LIBS = '-lgds -lsun';
}
elsif ($os eq 'aix')
{
    $LIBS = '-lgdsshr';
}
elsif ($os eq 'dgux')
{
    $LIBS = '-lgds -lgdsf -ldl -ldgc';
}
elsif ($os eq 'osf1')
{
    $LIBS = '-lgds';
}
elsif ($os eq 'sysv')
{
    $LIBS = '-lgds -lnsl -lsocket -ldl';
}
else
{
    carp "DBD::InterBase is not supported on platform $os.\n";
    exit 1;
}

make_test_conf;

WriteMakefile(
    'NAME'      => 'DBD::InterBase',
    'VERSION_FROM'  => 'InterBase.pm', # finds $VERSION
    'C'         => ['dbdimp.c'],
    'H'         => ['dbdimp.h', 'InterBase.h'],
    'CCFLAGS'   => $CCFLAGS,
    'INC'       => "$INC",
    'OBJECT'    => "InterBase.o dbdimp.o",
    'LIBS'      => [ $LIBS ],
    'OPTIMIZE'  => $OPT,
    'XSPROTOARG'    => '-noprototypes',
    'dist'      => {COMPRESS=>'gzip -9f', SUFFIX=>'gz'},
    'realclean' => '*.xsi test.conf',
);

package MY;

sub postamble
{
  &::dbd_postamble(@_);
}

package main;

__DATA__
CREATE DATABASE "__TESTDB__" user "__USER__" password "__PASS__";

quit;

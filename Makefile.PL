
BEGIN { $^W = 1 }
BEGIN { require 5.003 }	# 5.003 fixes very important bugs

use ExtUtils::MakeMaker 5.16, qw(prompt &WriteMakefile $Verbose);
use Config;
use Carp;

# This DBI must be installed before we can build a DBD.
# For those not using Dynamic loading this means building a
# new static perl in the DBI directory by saying 'make perl'
# and then using _that_ perl to make this one.
use DBI 1.08;
use DBI::DBD;	# DBD creation tools

my $dbi_arch_dir = dbd_dbi_arch_dir();

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

my $GFIX_PATH = '/usr/interbase/bin/gfix';
if (! -f $GFIX_PATH)
{
    $GFIX_PATH = '/opt/interbase/bin/gfix';
    if (! -f $GFIX_PATH)
    {
    $GFIX_PATH = '/Progra~1/InterB~1/InterB~1/bin/gfix';
    if (! -f $GFIX_PATH)
    {
	$GFIX_PATH = '/Progra~1/Borland/InterB~1/bin/gfix';
	if (! -f $GFIX_PATH)
	{
	    carp "I cannot find your InterBase installation.\nDBD::InterBase cannot build or run without InterBase.\n";
	    exit 1;
	}
    }
	}
}
my $IBVERSION;
chop($IBVERSION = `$GFIX_PATH -z 2>&1`);
$IBVERSION =~ s/^gfix version //o;

my $is_final = $IBVERSION =~ /\.6\d+$/ ? 1 : 0;
my $ib_inc = ($is_final ? 
	"/opt/interbase/include" : "/usr/interbase/include");
$ib_inc = prompt("Where is your InterBase include directory?",
	$ib_inc);

my $INC = "-I$ib_inc" . " -I$dbi_arch_dir";
my $LDDLFLAGS = '';
my $LIBS = '';
my $OPT = $Config::Config{'optimize'};

my $CCFLAGS = '';

my $os = $Config::Config{'osname'};

if ($os eq 'MSWin32')
{
    # Why does software development on Windows
    # have to be so difficult!?

    # Rules for Microsoft Visual C++
    if ($Config{'cc'} eq 'cl.exe') {
	$CCFLAGS = '-O2 -MD -DNDEBUG -DWIN32 -D_CONSOLE -DNO_STRICT -DHAVE_DES_FCRYPT';
	$LDFROM = '$(OBJECT) handles.obj util.obj';
	$INC = "-I$Config::Config{'archlibexp'}/CORE -I\"C:/Program Files/InterBase Corp/InterBase/include\" -I\"C:/Program Files/DevStudio/Vc/include\"";
	eval "
sub MY::const_loadlibs
{
'
LDLOADLIBS = gds32_ms.lib \$(LIBC)
LDDLFLAGS = -nodefaultlib /LIBPATH:C:/Progra~1/InterB~1/InterB~1/lib /LIBPATH:C:/Perl/lib/CORE
'
}
	";
    }
    # Rules for Borland C++
    else
    {
	$CCFLAGS = '-a4';
	$LDFROM = '$(OBJECT) handles.obj util.obj';
	$INC = "-I$Config::Config{'archlibexp'}/CORE -I\"C:/Program Files/InterBase Corp/InterBase/include\" -I\"C:/Program Files/Borland/BC5/include\"";
	eval "
sub MY::const_loadlibs {
'
LDLOADLIBS = \$(LIBC) import32.lib gds32.lib
LDDLFLAGS = -L\"C:/Progra~1/InterB~1/InterB~1/lib;C:/Progra~1/Borland/BC5/lib;C:/Perl/lib/CORE\"
'
}
	";
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
	my $ib_lib = ($is_final ? '/usr/lib ' : '/usr/interbase/lib ');

	$ib_lib = prompt("Where is your InterBase library directory?",
		$ib_lib);

    $LIBS = "-L$ib_lib" . '-lgds -ldl';

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
	my $ib_lib = ($is_final ? '/usr/lib ' : '/usr/interbase/lib ');

	$ib_lib = prompt("Where is your InterBase library directory?",
		$ib_lib);
    $LIBS = "-L$ib_lib" . '-lgds';
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

# Note: use -DIBSERVICES to enable the experimental code
# for InterBase 6 Services API.  This code is far from
# complete and is for development purposes only at this time.

WriteMakefile(
    'NAME'		=> 'DBD::InterBase',
    'VERSION_FROM' 	=> 'InterBase.pm', # finds $VERSION
	'C'         => ['dbdimp.c'],
  	'H'			=> ['dbdimp.h', 'InterBase.h'],
    'CCFLAGS'	=> $CCFLAGS,
    'INC'		=> "$INC",
	'OBJECT'    => "InterBase.o dbdimp.o",
    'LIBS'		=> [ $LIBS ],
    'OPTIMIZE'	=> $OPT,
    'XSPROTOARG'	=> '-noprototypes',
    'dist'		=> {COMPRESS=>'gzip -9f', SUFFIX=>'gz'},
	'realclean' => '*.xsi',
);

package MY;

sub postamble
{
  &::dbd_postamble(@_);
}

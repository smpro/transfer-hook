#!/usr/bin/perl -w

use strict;
use Getopt::Long;
use DBI;
use File::Basename;
use T0::Util;

my $help;
my $sender;
my ($delay, $config, $setuplabel, $time_t, $ofname, $fromrun, $uptorun, $forrun);
my @injl;

#########
my $exec = "/nfshome0/cmsprod/TransferTest/injection/sendNotification.sh";
########


sub usage
{
  $0 = basename($0);

  die <<EOF;

  Script queries the StorageManager database for files that are marked by the StorageManager as injected but which
  either are not known by the transfer system or have not been copied yet.

  An executable bash script with reinjection messages for these files is created.

  Usage $0 [--help] [--delay=<number>] [--config=<configuration>] [--setuplabel=<name>]
          [--time_t=<number>] [--ofname=<filename>] [--fromrun=<runnumber>] [--uptorun=<runnumber>]
          [--forrun=<runnumber>]

	--delay           sets sleeping time before next injection is made (in seconds)
			  default value is 0

        --config          Cessy transfer system configuration file (mandatory)

        --setuplabel      restricts the query to certain setuplabel (optional)

        --time_t          sets minimum time period between file is registered in SM DB
                          and execution moment for script to inject only those files
                          that older than value calculated as <localtime>-<delay>.
                          Default value is 60*60*3.

                          currently not used

	--ofname	  output file name to generate bash script for injection (mandatory)

        --fromrun         limits the query to runs greater or equal to this number

	--uptorun 	  limits the query to runs smaller or equal this number

        --forrun          limits the query to a particuar run (if uptorun not defined)

EOF
}

$help = 0;
$delay = 0;
$config = undef;
$setuplabel = undef;
$time_t = 60*60*3;
$ofname = undef;
$fromrun = undef;
$uptorun = undef;
$forrun = undef;
GetOptions(
           "help"          => \$help,
	   "delay=s"	   => \$delay,
	   "config=s"      => \$config,
	   "setuplabel=s"  => \$setuplabel,
	   "time_t=s"      => \$time_t,
	   "ofname=s"	   => \$ofname,
           "fromrun=s"     => \$fromrun,
	   "uptorun=s"	   => \$uptorun,
           "forrun=s"      => \$forrun,
	  );

$help && usage;

die "Config file is mandatory\n" if ( !defined($config) );
die "Need to specify output file\n" if ( !defined($ofname) );

### Query DB subroutine. Fills hash of files
sub GetFiles {

  my ($sql, $sth);

  my $dbHash = {};
  T0::Util::ReadConfig( $dbHash, , "TransferStatus::Worker", $config );

  my $databaseinstance = $dbHash->{DatabaseInstance};
  my $databasename = $dbHash->{DatabaseName};
  my $databaseuser = $dbHash->{DatabaseUser};
  my $databasepassword = $dbHash->{DatabasePassword};

  $sql = "SELECT a.filename,a.hostname,a.setuplabel,a.type,a.stream,";
  $sql .= "a.app_name,a.app_version,a.runnumber,a.lumisection,";
  $sql .= "b.pathname,b.destination,b.nevents,b.filesize,b.checksum,b.comment_str ";
  $sql .= "FROM " . $databasename . ".files_created a ";
  $sql .= "INNER JOIN " . $databasename . ".files_injected b ";
  $sql .= "ON a.filename = b.filename ";
#  $sql .= "INNER JOIN " . $databasename . ".files_trans_new c ";
#  $sql .= "ON a.filename = c.filename ";
  $sql .= "LEFT OUTER JOIN " . $databasename . ".files_trans_copied d ";
  $sql .= "ON a.filename = d.filename ";
  $sql .= "LEFT OUTER JOIN " . $databasename . ".files_deleted e ";
  $sql .= "ON a.filename = e.filename ";
  $sql .= "WHERE d.filename IS NULL ";
  $sql .= "AND e.filename IS NULL ";

  if ( defined($setuplabel) )
    {
      $sql .= "AND a.setuplabel = '" . $setuplabel . "' ";
    }
  if ( defined($fromrun) )
    {
      $sql .= "AND a.runnumber >= " . $fromrun . " ";
    }
  if ( defined($uptorun) )
    {
      $sql .= "AND a.runnumber <= " . $uptorun . " ";
    }
  elsif ( defined ($forrun) )
    {
      $sql .= "AND a.runnumber = " . $forrun . " ";
    }

  $sql .= "ORDER BY a.runnumber,a.lumisection ";

  # connect to database
  my $dbh = DBI->connect($databaseinstance,$databaseuser,$databasepassword);

  unless ( $dbh )
    {
      die "cannot connect to the DB: $DBI::errstr\n";
    };

  $sth = $dbh->prepare( $sql ) || die "failed prepare : $dbh->errstr\n";
  $sth->execute() || die "failed execute : $dbh->errstr\n";

  my $count = 0;
  while ( my @fd = $sth->fetchrow_array() ) {

    next unless defined($fd[0]);
    next unless defined($fd[1]);
    next unless defined($fd[2]);
    next unless defined($fd[3]);
    next unless defined($fd[4]);
    next unless defined($fd[5]);
    next unless defined($fd[6]);
    next unless defined($fd[7]);
    next unless defined($fd[8]);
    next unless defined($fd[9]);
    next unless defined($fd[10]);
    next unless defined($fd[11]);
    next unless defined($fd[12]);
    next unless defined($fd[14]); # this already has HLTKEY= in database

    my $row = "$exec --FILENAME=$fd[0] --HOSTNAME=$fd[1] --SETUPLABEL=$fd[2] --TYPE=$fd[3] --STREAM=$fd[4] --APP_NAME=$fd[5] --APP_VERSION=$fd[6] --RUNNUMBER=$fd[7] --LUMISECTION=$fd[8] --PATHNAME=$fd[9] --DESTINATION=$fd[10] --NEVENTS=$fd[11] --FILESIZE=$fd[12] --$fd[14]";

    if ( $fd[13] )
      {
	$row .= " --CHECKSUM=$fd[13]";
      }

    push @injl, $row;

    ++$count;
  };

  $sth->finish() || die "failed finish : $dbh->errstr\n";

};

### Get files
&GetFiles();

### Create bash script
my ($count, $sstr, $estr, $sleepstr, $env);
$sstr = "Executing statement";
$estr = "of " . @injl;
$sleepstr = "sleep $delay";
$count = 0;

open (FILE, ">$ofname") or die "$!\n";

map {print FILE "echo '$sstr ". (++$count) . " $estr' \n$_\n$sleepstr\n"} @injl;

close(FILE);

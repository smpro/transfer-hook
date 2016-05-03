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

sub usage
{
  $0 = basename($0);

  die <<EOF;

  Script queries the StorageManager database for files that have been copied but not checked,
  either are not known by the transfer system or have not been copied yet.

  New t0input.available notifications are written to a file.

  Usage $0 [--help] [--delay=<number>] [--config=<configuration>] [--setuplabel=<name>]
          [--time_t=<number>] [--ofname=<filename>] [--fromrun=<runnumber>] [--uptorun=<runnumber>]
          [--forrun=<runnumber>]

	--delay           sets sleeping time before next injection is made (in seconds)
			  default value is 0

        --config          Cessy transfer system configuration file (mandatory)

                          Extract database connection information and destination
                          rules to construct LFN from here.

        --setuplabel      restricts the query to certain setuplabel (optional)

        --time_t          sets minimum time period between file is registered in SM DB
                          and execution moment for script to inject only those files
                          that older than value calculated as <localtime>-<delay>.
                          Default value is 60*60*3.

                          currently not used

	--ofname	  output file name to write notifications to (mandatory)

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

  my $destHash = {};
  T0::Util::ReadConfig( $destHash, , "Copy::Worker", $config );

  my $dbHash = {};
  T0::Util::ReadConfig( $dbHash, , "TransferStatus::Worker", $config );

  my $databaseinstance = $dbHash->{DatabaseInstance};
  my $databasename = $dbHash->{DatabaseName};
  my $databaseuser = $dbHash->{DatabaseUser};
  my $databasepassword = $dbHash->{DatabasePassword};

  $sql = "SELECT a.filename,a.setuplabel,a.type,a.stream,";
  $sql .= "a.app_name,a.app_version,a.runnumber,a.lumisection,";
  $sql .= "b.pathname,b.destination,b.nevents,b.filesize,b.checksum,b.comment_str ";
  $sql .= "FROM " . $databasename . ".files_created a ";
  $sql .= "INNER JOIN " . $databasename . ".files_injected b ";
  $sql .= "ON a.filename = b.filename ";
  $sql .= "INNER JOIN " . $databasename . ".files_trans_copied c ";
  $sql .= "ON a.filename = c.filename ";
  $sql .= "LEFT OUTER JOIN " . $databasename . ".files_trans_checked d ";
  $sql .= "ON a.filename = d.filename ";
  $sql .= "WHERE d.filename IS NULL ";

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
  
  print "database instance: $databaseinstance,$databaseuser,$databasepassword \n";
  print "sql: $sql \n";

  unless ( $dbh )
    {
      die "cannot connect to the DB: $DBI::errstr\n";
    };

  $sth = $dbh->prepare( $sql ) || die "failed prepare : $dbh->errstr\n";
  $sth->execute() || die "failed execute : $dbh->errstr\n";

  
  #print "sth execute: $sth->fetchrow_array() \n";
  #my @zeynep = $sth->fetchrow_array();
  #print "zeynep @zeynep \n";

  while ( my @fd = $sth->fetchrow_array() ) {

    #print "Hi Zeynep are you here? \n";

    my $destination = $fd[9];

    # deciding what parameters set to apply according to the destination
    my $dsparams;
    if ( exists($destHash->{DestinationConfiguration}->{$destination}) ) {
      $dsparams = $destHash->{DestinationConfiguration}->{$destination};
    } else {
      $dsparams = $destHash->{DestinationConfiguration}->{default};
    }

    my $targetdir = $dsparams->{TargetDir};

    my $hash_ref = {};
    $hash_ref->{OnlineFile} = 't0input.available';

    my $filename = $fd[0];
    my $pathname = $fd[8];

    my $run = $fd[6];
    my $stream = $fd[3];
    my $setuplabel = $fd[1];

    $hash_ref->{RUNNUMBER} = $run;
    $hash_ref->{LUMISECTION} = $fd[7];
    $hash_ref->{NEVENTS} = $fd[10];
    $hash_ref->{START_TIME} = 0;
    $hash_ref->{STOP_TIME} = 0;
    $hash_ref->{SETUPLABEL} = $setuplabel;
    $hash_ref->{STREAM} = $stream;
    $hash_ref->{FILESIZE} = $fd[11];

    if ( $fd[12] )
      {
	$hash_ref->{CHECKSUM} = $fd[12];
      }
    else
      {
	$hash_ref->{CHECKSUM} = 0;
      }

    $hash_ref->{TYPE} = $fd[2];
    $hash_ref->{APP_NAME} = $fd[4];
    $hash_ref->{APP_VERSION} = $fd[5];

    my $hltkey = $fd[13];
    $hltkey =~ s/HLTKEY=//;
    $hash_ref->{HLTKEY} = $hltkey;

    $hash_ref->{DeleteAfterCheck} = $dsparams->{DeleteAfterCheck};
    $hash_ref->{SvcClass} = $dsparams->{SvcClass};
    $hash_ref->{T0FirstKnownTime} = 0;
    $hash_ref->{InjectIntoTier0} = 0;

    if ( $dsparams->{SplitMode} eq 'tier0StreamerNewPoolsLFN' ) {

	my $lfndir;

	if ( ( not defined($stream) ) or $stream eq '' ) {

	    $lfndir = sprintf("/store/t0streamer/%s/%03d/%03d/%03d", $setuplabel,
			      $run/1000000, ($run%1000000)/1000, $run%1000);
	} else {

	    $lfndir = sprintf("/store/t0streamer/%s/%s/%03d/%03d/%03d", $setuplabel, $stream,
			      $run/1000000, ($run%1000000)/1000, $run%1000);
	}

	$targetdir .= $lfndir;

	$hash_ref->{PFN} = $targetdir . '/' . $filename;
	$hash_ref->{LFN} = $lfndir . "/" . $filename;

	$hash_ref->{InjectIntoTier0} = 1;

    } elsif ( $dsparams->{SplitMode} eq 'tier0StreamerLFN' ) {

	my $lfndir;

	if ( ( not defined($stream) ) or $stream eq '' ) {

	    $lfndir = sprintf("/store/streamer/%s/%03d/%03d/%03d", $setuplabel,
			      $run/1000000, ($run%1000000)/1000, $run%1000);
	} else {

	    $lfndir = sprintf("/store/streamer/%s/%s/%03d/%03d/%03d", $setuplabel, $stream,
			      $run/1000000, ($run%1000000)/1000, $run%1000);
	}

	$targetdir .= $lfndir;

	$hash_ref->{PFN} = $targetdir . '/' . $filename;
	$hash_ref->{LFN} = $lfndir . "/" . $filename;

	$hash_ref->{InjectIntoTier0} = 1;

    } elsif ( $dsparams->{SplitMode} eq 'streamerLFN' ) {

	my $lfndir;

	if ( ( not defined($stream) ) or $stream eq '' ) {

	    $lfndir = sprintf("/store/streamer/%s/%03d/%03d/%03d", $setuplabel,
			      $run/1000000, ($run%1000000)/1000, $run%1000);
	} else {

	    $lfndir = sprintf("/store/streamer/%s/%s/%03d/%03d/%03d", $setuplabel, $stream,
			      $run/1000000, ($run%1000000)/1000, $run%1000);
	}

	$targetdir .= $lfndir;

	$hash_ref->{PFN} = $targetdir . '/' . $filename;
	$hash_ref->{LFN} = $lfndir . "/" . $filename;

    } else {

	print "File " . $filename . " has splitMode " . $dsparams->{SplitMode} . " , not supported, skip this file\n";
	next;
    }

    my $time = scalar localtime;
    print $time;
    push @injl, $time . ": { " . join(', ', map { "'$_' => '$hash_ref->{$_}'" } keys %$hash_ref) . " }";
  };

  $sth->finish() || die "failed finish : $dbh->errstr\n";
};

### Get files
&GetFiles();

### Create notification script

open (FILE, ">$ofname") or die "$!\n";

map {print FILE "$_\n"} @injl;

close(FILE);

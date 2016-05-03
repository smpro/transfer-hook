#!/usr/bin/perl -w

use strict;
use Getopt::Long;
use DBI;
use File::Basename;

my $help;

sub usage
{
  $0 = basename($0);

  die <<EOF;

  Usage $0 [--help]

  Prints lines for resend.txt to stdout, redirect to file to catch output

EOF
}

$help = 0;
GetOptions(
           "help"   => \$help,
	  );

$help && usage;

###
### Script extracts streamer info from T0AST and creates Tier0Inject messages
###

### Query DB subroutine.
sub GetFiles {

  my ($sql, $sth);

  my $databaseinstance = "dbi:Oracle:host=oradev10.cern.ch;sid=D10;port=10520";
  my $databasename = "CMS_T0AST_DIRK";
  my $databaseuser = "CMS_T0AST_DIRK";
  my $databasepassword = "replace_with_password";

  $sql = "SELECT a.lfn, a.run_id, a.lumi_id, a.events, a.filesize, a.indexpfn, b.name, c.hltkey, d.name ";
  $sql .= "FROM " . $databasename . ".streamer a ";
  $sql .= "INNER JOIN " . $databasename . ".stream b ";
  $sql .= "ON b.id = a.stream_id ";
  $sql .= "INNER JOIN " . $databasename . ".run c ";
  $sql .= "ON c.run_id = a.run_id ";
  $sql .= "INNER JOIN " . $databasename . ".cmssw_version d ";
  $sql .= "ON d.id = c.run_version ";

  # connect to database
  my $dbh = DBI->connect($databaseinstance,$databaseuser,$databasepassword);

  unless ( $dbh )
    {
      die "cannot connect to the DB: $DBI::errstr\n";
    };

  $sth = $dbh->prepare( $sql ) || die "failed prepare : $dbh->errstr\n";
  $sth->execute() || die "failed execute : $dbh->errstr\n";

  while ( my @fd = $sth->fetchrow_array() ) {

    my $hash_ref = {};

    $hash_ref->{Tier0Inject} = 1;

    $hash_ref->{LFN} = $fd[0];
    $hash_ref->{RUNNUMBER} = $fd[1];
    $hash_ref->{LUMISECTION} = $fd[2];
    $hash_ref->{NEVENTS} = $fd[3];
    $hash_ref->{FILESIZE} = $fd[4];
    $hash_ref->{INDEXPFN} = $fd[5];
    $hash_ref->{STREAM} = $fd[6];
    $hash_ref->{HLTKEY} = $fd[7];
    $hash_ref->{APP_VERSION} = $fd[8];

    $hash_ref->{CHECKSUM} = 0;
    $hash_ref->{START_TIME} = 0;
    $hash_ref->{STOP_TIME} = 0;
    $hash_ref->{T0FirstKnownTime} = 0;
    $hash_ref->{PFN} = "/castor/cern.ch/cms" . $hash_ref->{LFN};
    $hash_ref->{TYPE} = "streamer";

    my $time = scalar localtime;
    print $time,': { ';

    print join(', ', map { "'$_' => '$hash_ref->{$_}'" } keys %$hash_ref);

    print " }\n";
  };

  $sth->finish() || die "failed finish : $dbh->errstr\n";
};

### Get files
&GetFiles();


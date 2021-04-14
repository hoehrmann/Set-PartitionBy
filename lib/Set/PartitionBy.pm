#!/usr/bin/env perl
package Set::PartitionBy;
use 5.018000;
use strict;
use warnings;
use DBI;
use Log::Any;
use Moo;
use Types::Standard qw/:all/;
use Tie::RefHash;

our $VERSION = '0.01';

has 'storage_dsn' => (
  is       => 'ro',
  required => 1,
  isa      => Str,
  default  => sub {
    'dbi:SQLite:dbname=:memory:'
  },
);

has '_dbh' => (
  is       => 'ro',
  required => 0,
  writer   => '_set_dbh',
);

has '_log' => (
  is       => 'rw',
  required => 0,
  default  => sub {
    Log::Any->get_logger()
  },
);

has '_element_to_id' => (
  is       => 'rw',
  required => 0,
  default  => sub {
    tie my %h => 'Tie::RefHash';
    return \%h;
  },
);

has 'elements' => (
  is       => 'rw',
  required => 0,
  isa      => ArrayRef,
);

has '_once_by' => (
  is       => 'rw',
  required => 0,
  isa      => ArrayRef[CodeRef],
  default  => sub { [] },
);

has '_then_by' => (
  is       => 'rw',
  required => 0,
  isa      => ArrayRef[CodeRef],
  default  => sub { [] },
);

has '_round' => (
  is       => 'rw',
  required => 0,
  isa      => Int,
  default  => 0,
);

sub BUILD {
  my ($self) = @_;

  $self->_log->debugf("Creating database with %s", $self->storage_dsn);

  my $dbh = DBI->connect( $self->storage_dsn );

  $dbh->{RaiseError} = 1;

  $self->_set_dbh( $dbh );

  $self->_deploy_schema;

}

sub _deploy_schema {
  my ($self) = @_;

  local $self->_dbh->{sqlite_allow_multiple_statements} = 1;

  $self->_dbh->do(q{
    PRAGMA foreign_keys = OFF;
    PRAGMA synchronous = OFF;
    PRAGMA journal_mode = OFF;
    PRAGMA locking_mode = EXCLUSIVE;

    DROP TABLE IF EXISTS history;
    CREATE TABLE history(
      element INT NOT NULL,
      src INT NOT NULL,
      dst INT NOT NULL,
      round INT NOT NULL
    );

    DROP TABLE IF EXISTS tracking;
    CREATE TABLE tracking(
      element INTEGER PRIMARY KEY,
      partition INT NOT NULL DEFAULT 1,
      round INT NOT NULL DEFAULT 1
    );

    CREATE INDEX tracking_partition
      ON tracking(partition);

    CREATE TRIGGER trigger_tracking_history
    AFTER UPDATE ON tracking
    BEGIN
      INSERT INTO history(element, src, dst, round)
      VALUES(old.element, old.partition, new.partition, new.round);
    END;

  });

}

sub _transaction {
 
  my ($self, $sub) = @_;

  $self->_dbh->begin_work;
  $sub->();
  $self->_dbh->commit;

}

sub partition {

  my ($self, @elements) = @_;

  die if $self->elements;

  @{ $self->_element_to_id }{ @elements } = 1 .. @elements;

  $self->elements(\@elements);

  my $insert_sth = $self->_dbh->prepare(q{
    INSERT OR IGNORE INTO tracking(element) VALUES(?)
  });

  $self->_transaction(sub {
    $insert_sth->execute($_) for sort values %{ $self->_element_to_id };
  });

  return $self;

}

sub once_by {
  my ($self, $sub) = @_;

  push @{ $self->_once_by }, $sub;

  return $self;
}

sub then_by {
  my ($self, $sub) = @_;

  push @{ $self->_then_by }, $sub;

  return $self;
}

sub refine {

  my ($self) = @_;

  my $sub = sub { };

  $self->_dbh->sqlite_create_function('partition_by', 1, sub {

    my ($element) = @_;

    local $_ = $self->elements->[ $element - 1 ];

    return $sub->( $_ );

  });

  my $update_sth = $self->_dbh->prepare(q{
    WITH
    refinement AS (
      SELECT
        element,
        DENSE_RANK() OVER w AS partition
      FROM
        tracking
      WINDOW
        w AS (
          ORDER BY partition_by(element), partition
          GROUPS CURRENT ROW
        )
    )
    UPDATE
      tracking
    SET
      round = (SELECT MAX(round) FROM tracking) + 1,
      partition = refinement.partition
    FROM
      refinement
    WHERE
      refinement.element = tracking.element
      AND
      refinement.partition <> tracking.partition
  });

  if (@{ $self->_once_by }) {

    $sub = shift @{ $self->_once_by };

  } elsif (@{ $self->_then_by }) {

    $sub = $self->_then_by->[ $self->_round % @{ $self->_then_by } ];
  }

  my $ra;

  $self->_transaction(sub {
    $ra = $update_sth->execute();
    $self->_round( $self->_round + 1 );
  });

  return $ra > 0;
}

sub peers {

  my ($self, $element) = @_;

  return $self->to_elements( $self->to_partition( $element) );

}

sub to_elements {
  
  my ($self, $partition) = @_;

  return
    map { $self->elements->[ $_ - 1 ] }
    map { @$_ }
    $self->_dbh->selectall_array(q{

    SELECT element FROM tracking WHERE partition = ? + 0      

  }, {}, $partition);

}

sub to_partition {
  
  my ($self, $element) = @_;

  return
    $self->_dbh->selectrow_array(q{

    SELECT partition FROM tracking WHERE element = ? + 0      

  }, {}, $self->_element_to_id->{ $element} );

}

sub last_common {

  my ($self, $element1, $element2) = @_;

  return $self->_dbh->selectrow_array(q{

    SELECT
      COALESCE(MAX(a.dst), 1) AS partition,
      COALESCE(MAX(a.round), 1) AS round
    FROM
      history a JOIN history b ON a.dst = b.dst
    WHERE
      a.element = ? + 0 and b.element = ? + 0

  }, {}, map { $self->_element_to_id->{$_} } $element1, $element2);

}

sub partition_tree {
  
  my ($self) = @_;

  $self->_dbh->selectall_array(q{
    SELECT DISTINCT src, dst FROM history
  });

}

sub history {

  my ($self) = @_;

  return map { $_->[0] = $self->elements->[ $_->[0] ]; $_ }
    $self->_dbh->selectall_array(q{
    SELECT element, src, dst, round FROM history
  });

}

1;

__END__

package main;

my $p = Set::PartitionBy->new(
  storage_dsn => 'dbi:SQLite:dbname=delme.db'
);

$p->partition(1, 2, 7, 12, 31, 16, 3, 4, 9, 63)
  ->once_by(sub { $_ & 0b0001 })
  ->once_by(sub { $_ & 0b0010 })
  ->once_by(sub { $_ & 0b0100 })
  ->once_by(sub { $_ & 0b1000 })
  ;

while ($p->refine) {
  1
}

__END__

$p->to_graph







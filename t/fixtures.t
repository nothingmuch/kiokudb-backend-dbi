#!/usr/bin/perl

use Test::More 'no_plan';
use Test::TempDir;

use ok 'KiokuDB';
use ok 'KiokuDB::Backend::DBI';

use KiokuDB::Test;

use DBIx::Class::Storage::DBI;

my $storage = "DBIx::Class::Storage::DBI"->new;

#$storage->connect_info([ 'dbi:mysql:test' ]);
$storage->connect_info([ 'dbi:SQLite:dbname=' . temp_root->file('db') ]);

$storage->txn_do(sub {
    $storage->dbh->do("drop table if exists entries");
    $storage->dbh->do("drop table if exists gin_index");
    $storage->dbh->do("CREATE TABLE entries (
        id VARCHAR(255) PRIMARY KEY,
        data BLOB NOT NULL,
        class VARCHAR(255) NOT NULL,
        root BOOLEAN NOT NULL,
        tied BOOLEAN NOT NULL,
        oi VARCHAR(255)
    ) -- TYPE=InnoDB");
    $storage->dbh->do("CREATE TABLE gin_index (
        id VARCHAR(255),
        value VARCHAR(255)
    ) -- TYPE=InnoDB");
    $storage->dbh->do("CREATE INDEX gin_index_ids ON gin_index ( id )");
    $storage->dbh->do("CREATE INDEX gin_index_values ON gin_index ( value )");
});

use Search::GIN::Extract::Class;

my $b = KiokuDB::Backend::DBI->new(
    storage => $storage,
    columns => [qw(oi)],
    extract => Search::GIN::Extract::Class->new,
    root_only => 0,
);

run_all_fixtures( KiokuDB->new( backend => $b ) );

#run_all_fixtures( KiokuDB->connect("dbi:SQLite:dbname=" . temp_root->file("db")) );



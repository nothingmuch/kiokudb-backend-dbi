#!/usr/bin/perl

use strict;
use warnings;

use Test::More 'no_plan';

use ok 'KiokuDB::Backend::DBI';
use ok 'KiokuDB::Entry';

use Test::TempDir;
use DBIx::Class::Storage::DBI;

my $storage = "DBIx::Class::Storage::DBI"->new;

#$storage->connect_info([ 'dbi:mysql:test' ]);
$storage->connect_info([ 'dbi:SQLite:dbname=' . temp_root->file("db") ]);

$storage->txn_do(sub {
    $storage->dbh->do("drop table if exists entries");
    $storage->dbh->do("CREATE TABLE entries (
        id VARCHAR(255) PRIMARY KEY,
        data BLOB NOT NULL,
        class VARCHAR(255) NOT NULL,
        root BOOLEAN NOT NULL,
        tied BOOLEAN NOT NULL,
        oi VARCHAR(255)
    ) -- TYPE=InnoDB");
});

my $b = KiokuDB::Backend::DBI->new(
    storage => $storage,
    columns => [qw(oi)],
);

my $entry = KiokuDB::Entry->new(
    id => "foo",
    root => 1,
    class => "Foo",
    data => { oi => "vey" },
);

my $row = $b->entry_to_row($entry);

is( $row->{id}, $entry->id, "ID" );

is( $row->{class}, $entry->class, "class" );

ok( $row->{root}, "root entry" );

like( $row->{data}, qr/oi.*vey/, "JSON data" );

ok( exists $row->{oi}, "extracted column" );

is( $row->{oi}, "vey", "column data" );

$b->txn_do(sub {
    $b->insert( $entry );
});

my ( $loaded_entry ) = $b->get("foo");

isnt( $loaded_entry, $entry, "entries are different" );

is_deeply( $loaded_entry, $entry, "but eq deeply" );

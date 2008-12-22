#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

BEGIN {
    plan skip_all => "DBD::SQLite is required" unless eval { require DBI; require DBD::SQLite };
    plan 'no_plan';
}

use Test::TempDir;

use ok 'KiokuDB::Backend::DBI';
use ok 'KiokuDB::Entry';

my $b = KiokuDB::Backend::DBI->new(
    dsn => 'dbi:SQLite:dbname=' . temp_root->file("db"),
    columns => [qw(oi)],
);

$b->deploy;

my $entry = KiokuDB::Entry->new(
    id => "foo",
    root => 1,
    class => "Foo",
    data => { oi => "vey" },
);

my $row = $b->entry_to_row($entry);

is( $row->[0], $entry->id, "ID" );

is( $row->[1], $entry->class, "class" );

ok( $row->[2], "root entry" );

like( $row->[4], qr/vey/, "data" );

ok( exists $row->[-1], "extracted column" );

is( $row->[-1], "vey", "column data" );

$b->txn_do(sub {
    $b->insert( $entry );
});

my ( $loaded_entry ) = $b->get("foo");

isnt( $loaded_entry, $entry, "entries are different" );

is_deeply( $loaded_entry, $entry, "but eq deeply" );

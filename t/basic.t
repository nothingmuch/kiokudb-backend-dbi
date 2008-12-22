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

#!/usr/bin/perl

use Test::More 'no_plan';
use Test::TempDir;

use ok 'KiokuDB';
use ok 'KiokuDB::Backend::DBI';

use KiokuDB::Test;

use Search::GIN::Extract::Class;

my $dir = KiokuDB->connect(
    "dbi:SQLite:dbname=" . temp_root->file("db"),
    #"dbi:mysql:test",
    columns => [
        oi => {
            is_nullable => 1,
            data_type   => "varchar",
        }
    ],
    extract => Search::GIN::Extract::Class->new,
);

#$dir->backend->deploy({ add_drop_table => 1, producer_args => { mysql_version => 5 } });
$dir->backend->deploy;

run_all_fixtures($dir);



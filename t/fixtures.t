#!/usr/bin/perl

use Test::More;

BEGIN {
    plan skip_all => "DBD::SQLite is required" unless eval { require DBI; require DBD::SQLite };
    plan 'no_plan';
}

use Test::TempDir;

use ok 'KiokuDB';
use ok 'KiokuDB::Backend::DBI';

use KiokuDB::Test;

use Search::GIN::Extract::Class;

foreach my $dsn (
    [ "dbi:SQLite:dbname=" . temp_root->file("db") ],
    #[ "dbi:mysql:test" ],
    #[ "dbi:Pg:dbname=test" ],
) {
    warn "@$dsn";
    my $dir = KiokuDB->connect(
        @$dsn,
        columns => [
            name => {
                is_nullable => 1,
                data_type   => "varchar",
            },
            age => {
                is_nullable => 1,
                data_type   => "integer",
            },
        ],
        extract => Search::GIN::Extract::Class->new,
    );

    #$dir->backend->deploy({ add_drop_table => 1, producer_args => { mysql_version => 5 } });
    $dir->backend->deploy;

    run_all_fixtures($dir);
}

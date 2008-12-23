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

my $sqlite = "dbi:SQLite:dbname=" . temp_root->file("db");

my $dbh = DBI->connect($sqlite);

# disabling print_error doesn't make add_drop_table shut up
$dbh->do("CREATE TABLE entries ( id integer primary key )" );
$dbh->do("CREATE TABLE gin_index ( id integer primary key )" );

foreach my $dsn (
    [ $sqlite ],
    #[ "dbi:mysql:test" ],
    #[ "dbi:Pg:dbname=test" ],
) {
    foreach my $serializer (qw(json storable), eval { require YAML::XS; "yaml" }) {
        #diag "testing against $dsn->[0] with $serializer\n";

        my $dir = KiokuDB->connect(
            @$dsn,
            serializer => $serializer,
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

        $dir->backend->deploy({ add_drop_table => 1, producer_args => { mysql_version => 5 } });

        run_all_fixtures($dir);
    }
}

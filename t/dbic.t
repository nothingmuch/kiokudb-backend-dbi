#!/usr/bin/perl

use strict;
use warnings;

use Scalar::Util qw(refaddr);
use Test::More;
use Test::Exception;

use KiokuDB;

BEGIN {
    plan skip_all => "DBD::SQLite  are required" unless eval { require DBI; require DBD::SQLite };
}

{
    package MyApp::DB::Result::Foo;
    use base qw(DBIx::Class);

    __PACKAGE__->load_components(qw(Core KiokuDB));
    __PACKAGE__->table('foo');
    __PACKAGE__->add_columns(qw(id name object));
    __PACKAGE__->set_primary_key('id');
    __PACKAGE__->kiokudb_column('object');

    package MyApp::DB;
    use base qw(DBIx::Class::Schema);

    __PACKAGE__->load_components(qw(Schema::KiokuDB));

    __PACKAGE__->register_class( Foo => qw(MyApp::DB::Result::Foo));

    package Foo;
    use Moose;

    has name => ( isa => "Str", is => "ro" );
    has obj  => ( isa => "Object", is => "ro", weak_ref => 1 );

    __PACKAGE__->meta->make_immutable;
}

my $dir = KiokuDB->connect(
    'dbi:SQLite:dbname=:memory:',
    schema => "MyApp::DB",
    create => 1,
);

$dir->txn_do( scope => 1, body => sub {
    $dir->insert( foo => my $obj = Foo->new );

    $dir->backend->schema->resultset("Foo")->create({ id => 1, name => "foo", object => $obj });

    my $row = $dir->backend->schema->resultset("Foo")->create({ id => 2, name => "foo", object => "foo" });

    isa_ok( $row->object, 'Foo', 'inflated from constructor' );
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

foreach my $id ( 1, 2 ) {
    $dir->txn_do( scope => 1, body => sub {
        my $row = $dir->backend->schema->resultset("Foo")->find(1);

        isa_ok( $row, "MyApp::DB::Result::Foo" );

        isa_ok( $row->object, "Foo" );
        is( $dir->object_to_id( $row->object ), "foo", "kiokudb ID" );
    });
}

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    $dir->backend->schema->resultset("Foo")->create({ id => 3, name => "foo", object => Foo->new });
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    my $row = $dir->backend->schema->resultset("Foo")->find(3);

    isa_ok( $row, "MyApp::DB::Result::Foo" );

    isa_ok( $row->object, "Foo" );
    isnt( $dir->object_to_id( $row->object ), "foo", "kiokudb ID" );

    $row->object( Foo->new );

    isa_ok( $row->object, "Foo", "weakened object with no other refs" );

    throws_ok {
        $row->update;
    } qr/not in storage/, "can't update object without related KiokuDB objects being in storage";

    lives_ok { $row->store } "store method works";
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    my $row = $dir->backend->schema->resultset("Foo")->find(1);

    my $foo = Foo->new( obj => $row );

    $dir->insert( with_dbic => $foo );

});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    my $foo = $dir->lookup("with_dbic");

    isa_ok( $foo->obj, "DBIx::Class::Row" );
    is( $foo->obj->id, 1, "ID" );
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    ok( $dir->exists('dbic:row:["Foo",3]'), "dbic row exists" );

    my $foo = $dir->lookup('dbic:row:["Foo",3]');

    isa_ok( $foo, "DBIx::Class::Row" );
    is( $foo->id, 3, "ID" );
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    my $row = $dir->backend->schema->resultset("Foo")->find(2);

    my $foo = Foo->new( obj => $row );

    $dir->insert( another => $foo );

});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    # to cover the ->search branch (as opposed to ->find)
    my @foo = $dir->lookup("with_dbic", "another");

    isa_ok( $foo[0]->obj, "DBIx::Class::Row" );
    is( $foo[0]->obj->id, 1, "ID" );
    isa_ok( $foo[1]->obj, "DBIx::Class::Row" );
    is( $foo[1]->obj->id, 2, "ID" );
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    my $obj = $dir->backend->schema->resultset("entries")->find('with_dbic');

    is( $dir->object_to_id($obj), 'with_dbic', "object to ID of row fetched using 'find'");
    isa_ok( $obj, "Foo" );
    isa_ok( $obj->obj, "DBIx::Class::Row" );
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    my $rs = $dir->backend->schema->resultset("Foo")->search({ id => [ 1, 3 ] });

    my $foo = Foo->new( obj => $rs );

    $dir->insert( with_rs => $foo );
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    my $foo = $dir->lookup("with_rs");

    isa_ok( $foo, "Foo" );

    my $rs = $foo->obj;

    isa_ok( $rs, "DBIx::Class::ResultSet" );

    is( refaddr($rs->result_source->schema), refaddr($dir->backend->schema), "schema restored in resultset handle" );

    is_deeply(
        [ sort { $a->id <=> $b->id } $rs->all ],
        [ sort { $a->id <=> $b->id } $dir->backend->schema->resultset("Foo")->search({ id => [ 1, 3 ]})->all ],
        "result set works"
    );
});

is_deeply( [ $dir->live_objects->live_objects ], [], "no live objects" );

$dir->txn_do( scope => 1, body => sub {
    my $rs = $dir->backend->schema->resultset("Foo")->search({ id => [ 1, 3 ] });

    my $foo = Foo->new( obj => $dir->backend->schema );

    $dir->insert( with_schema => $foo );
});

# FIXME register it as immutable
is_deeply( [ $dir->live_objects->live_objects ], [ $dir->backend->schema ], "only schema in live objects" );
$dir->live_objects->clear;

$dir->txn_do( scope => 1, body => sub {
    my $foo = $dir->lookup("with_schema");

    isa_ok( $foo, "Foo" );

    my $rs = $foo->obj;

    my $schema = $foo->obj;

    isa_ok( $schema, "DBIx::Class::Schema" );

    is( refaddr($schema), refaddr($dir->backend->schema), "schema restored" );
});

is_deeply( [ $dir->live_objects->live_objects ], [ $dir->backend->schema ], "only schema in live objects" );
$dir->live_objects->clear;

done_testing;

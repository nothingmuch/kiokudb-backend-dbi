#!/usr/bin/perl

use strict;
use warnings;

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

    __PACKAGE__->meta->make_immutable;
}

my $dir = KiokuDB->connect(
    'dbi:SQLite:dbname=:memory:',
    schema_proto => "MyApp::DB",
    create => 1,
);

$dir->txn_do( scope => 1, body => sub {
    $dir->insert( foo => my $obj = Foo->new );

    $dir->backend->schema->resultset("Foo")->create({ id => 1, name => "foo", object => $obj });

    my $row = $dir->backend->schema->resultset("Foo")->create({ id => 2, name => "foo", object => "foo" });

    isa_ok( $row->object, 'Foo', 'inflated from constructor' );
});

foreach my $id ( 1, 2 ) {
    $dir->txn_do( scope => 1, body => sub {
        my $row = $dir->backend->schema->resultset("Foo")->find(1);

        isa_ok( $row, "MyApp::DB::Result::Foo" );

        isa_ok( $row->object, "Foo" );
        is( $dir->object_to_id( $row->object ), "foo", "kiokudb ID" );
    });
}

$dir->txn_do( scope => 1, body => sub {
    $dir->backend->schema->resultset("Foo")->create({ id => 3, name => "foo", object => Foo->new });
});

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

done_testing;

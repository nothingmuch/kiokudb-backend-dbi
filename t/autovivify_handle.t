#!/usr/bin/perl

use strict;
use warnings;

use Scalar::Util qw(refaddr);
use Test::More;use Test::Exception;
use Test::TempDir;
use KiokuDB;

BEGIN {
    plan skip_all => "DBD::SQLite and SQL::Translator >= 0.11005 are required"
        unless eval "use DBI; use DBD::SQLite; use DBIx::Class::Optional::Dependencies; 1";

    plan skip_all => DBIx::Class::Optional::Dependencies->req_missing_for("deploy")
        unless DBIx::Class::Optional::Dependencies->req_ok_for("deploy");
}

{
    package MyApp::DB::Result::Gaplonk; # Acme::MetaSyntacic::donmartin ++
    use base qw(DBIx::Class::Core);

    __PACKAGE__->load_components(qw(KiokuDB));
    __PACKAGE__->table('gaplonk');
    __PACKAGE__->add_columns(qw(id name object));
    __PACKAGE__->set_primary_key('id');
    __PACKAGE__->kiokudb_column('object');

    package MyApp::DB;
    use base qw(DBIx::Class::Schema);

    __PACKAGE__->load_components(qw(Schema::KiokuDB));
    __PACKAGE__->define_kiokudb_schema();

    __PACKAGE__->register_class( Gaplonk => qw(MyApp::DB::Result::Gaplonk));

    package Patawee;
    use Moose;

    has sproing => ( isa => "Str", is => "ro" );
    __PACKAGE__->meta->make_immutable;
}

my $sqlite = "dbi:SQLite:dbname=" . temp_root->file("db");
my $schema = MyApp::DB->connect($sqlite);

{
    my $refaddr;

    {
        isa_ok( my $k = $schema->kiokudb_handle, "KiokuDB" );
        $refaddr = refaddr($k);
    }

    {
        is( refaddr($schema->kiokudb_handle), $refaddr, "KiokuDB handle not weak when autovivified" );
    }
}

my $dir = $schema->kiokudb_handle;
isa_ok( $dir, 'KiokuDB', 'got autovived directory handle from schema');
$dir->backend->deploy;

my $id;
lives_ok {
    $dir->txn_do( scope => 1, body => sub {

        my $object = Patawee->new( sproing=> 'kalloon' );

        my $thing = $schema->resultset('Gaplonk')->create({
            id => 1,
            name =>'VOOMAROOMA',
            object => $object
        });
        $id =  $thing->id;
    });
} 'create row in DB';

$dir->txn_do( scope => 1, body => sub {
    my $fetch_again = $schema->resultset('Gaplonk')->find( $id );
    isa_ok( $fetch_again, 'MyApp::DB::Result::Gaplonk', 'got DBIC row object back' );
    is($fetch_again->name,'VOOMAROOMA','row->name');

    my $object = $fetch_again->object;
    isa_ok( $object, 'Patawee' );
    is( $object->sproing, 'kalloon', 'object attribute' );
});

done_testing();


#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use JSON;
use JSONPath;

my $j = JSON->new;
my $jp = JSONPath->new;

my $dbh = DBI->connect("dbi:SQLite::memory:", undef, undef, { RaiseError => 1 });

$dbh->sqlite_create_function( json_path => 2 => sub {
    my ( $json, $path ) = @_;

    my $data = ref $json ? $json : $j->decode($json);

    my $res = $jp->run($data, $path) or return;

    warn "SQLite can't handle multiple values from JSON path, only returning first result" if @$res > 1;

    return $res->[0];
});

$dbh->do("create table foo ( id integer primary key, data text )");
my $sth = $dbh->prepare("insert into foo values ( ?, ? )");

$sth->execute( 1, $j->encode({foo => "bar", blah => [ 1 .. 3 ]}) );
$sth->execute( 2, $j->encode({ blah => {foo => "bar"}, oink => { foo => "bar" }}) );

use Data::Dumper;
warn Dumper( $dbh->selectall_arrayref("select id, data, json_path(data, ?) from foo", undef, '$..foo') );


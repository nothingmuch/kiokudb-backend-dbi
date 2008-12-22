#!/usr/bin/perl

package KiokuDB::Backend::DBI;
use Moose;

use MooseX::Types -declare => [qw(ValidColumnName)];

use MooseX::Types::Moose qw(ArrayRef HashRef Str);

use Data::Stream::Bulk::DBI;

use KiokuDB::Backend::DBI::Schema;

use namespace::clean -except => 'meta';

our $VERSION = "0.01";

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::JSON
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::TXN
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::UnicodeSafe
    KiokuDB::Backend::Role::Query::Simple
    KiokuDB::Backend::Role::Query::GIN
    Search::GIN::Extract::Delegate
);
# KiokuDB::Backend::Role::TXN::Nested is not supported by many DBs
# we don't really care though

my %std_cols = ( map { $_ => 1 } qw(
    id
    tied
    class
    root
) );

my %reserved_cols = ( %std_cols, data => 1 );

subtype ValidColumnName, as Str, where { not exists $reserved_cols{$_} };

sub new_from_dsn {
    my ( $self, $dsn, @args ) = @_;
    $self->new( dsn => "dbi:$dsn", @args );
}

has '+utf8' => ( default => 1 );

has [qw(dsn user password)] => (
    isa => "Str",
    is  => "ro",
);

has dbi_attrs => (
    isa => HashRef,
    is  => "ro",
);

has connect_info => (
    isa => ArrayRef,
    is  => "ro",
    lazy_build => 1,
);

sub _build_connect_info {
    my $self = shift;

    return [ $self->dsn, $self->user, $self->password, $self->dbi_attrs ];
}

has schema => (
    isa => "DBIx::Class::Schema",
    is  => "ro",
    lazy_build => 1,
    handles => [qw(deploy)],
);

sub _build_schema {
    my $self = shift;

    my $schema = KiokuDB::Backend::DBI::Schema->clone;

    $schema->source("entries")->add_columns(@{ $self->columns });

    $schema->connect(@{ $self->connect_info });
}

has storage => (
    isa => "DBIx::Class::Storage::DBI",
    is  => "rw",
    lazy_build => 1,
    handles    => [qw(dbh dbh_do)],
);

sub _build_storage { shift->schema->storage }

has columns => (
    isa => ArrayRef[ValidColumnName|HashRef],
    is  => "ro",
    default => sub { [] },
);

has _columns => (
    isa => HashRef,
    is  => "ro",
    lazy_build => 1,
);

sub _build__columns {
    my $self = shift;

    my $rs = $self->schema->source("entries");

    my @user_cols = grep { not exists $reserved_cols{$_} } $rs->columns;

    return { map { $_ => $rs->column_info($_)->{extract} || undef } @user_cols };
}

has '+extract' => (
    required => 0,
);

has sql_abstract => (
    isa => "SQL::Abstract",
    is  => "ro",
    lazy_build => 1,
);

sub _build_sql_abstract {
    my $self = shift;

    SQL::Abstract->new;
}

has prepare_insert_method => (
    isa => "Str|CodeRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build_prepare_insert_method {
    my $self = shift;

    my $name = $self->storage->dbh->{Driver}{Name};

    if ( $self->can("prepare_${name}_insert") ) {
        return "prepare_${name}_insert";
    } else {
        "prepare_fallback_insert";
    }
}

sub insert {
    my ( $self, @entries ) = @_;

    my @rows = $self->entries_to_rows(@entries);

    $self->insert_rows(@rows);

    # hopefully we're in a transaction, otherwise this totally sucks
    if ( $self->extract ) {
        my %gin_index;

        foreach my $entry ( @entries ) {
            my $id = $entry->id;
            if ( $entry->deleted || !$entry->has_object ) {
                $gin_index{$id} = [];
            } else {
                my $d = $entry->backend_data || $entry->backend_data({});
                $gin_index{$id} = [ $self->extract_values( $entry->object, entry => $entry ) ];
            }
        }

        $self->update_index(\%gin_index);
    }
}

sub entries_to_rows {
    my ( $self, @entries ) = @_;

    map { $self->entry_to_row($_) } @entries;
}

sub entry_to_row {
    my ( $self, $entry ) = @_;

    my %row = map { $_ => $entry->$_ } keys %std_cols;

    for ( values %row ) {
        $_ = 0 unless defined;
    }

    $row{data} = $self->serialize($entry);

    if ( ref( my $data = $entry->data ) eq 'HASH' ) {
        my $cols = $self->_columns;
        foreach my $column ( keys %$cols ) {
            if ( my $extract = $cols->{$column} ) {
                if ( my $obj = $entry->object ) {
                    $row{$column} = $obj->$extract($column);
                }
            } else {
                if ( exists $data->{$column} and not ref( my $value = $data->{$column} ) ) {
                    $row{$column} = $value;
                }
            }
        }
    }

    return \%row;
}

sub insert_rows {
    my ( $self, @rows ) = @_;

    my ( $del, $ins, @bind ) = $self->prepare_insert();

    foreach my $row ( @rows ) {
        $del->execute( $row->{id} ) if $del;
        $ins->execute( @{ $row }{@bind} );
    }

    $del && $del->finish;
    $ins->finish;
}

sub prepare_insert {
    my $self = shift;

    my $meth = $self->prepare_insert_method;
    $self->$meth;
}

sub prepare_SQLite_insert {
    my $self = shift;

    my @cols = ( keys %reserved_cols, keys %{ $self->_columns } );

    my $sth = $self->dbh->prepare_cached("INSERT OR REPLACE INTO entries (" . join(", ", @cols) . ") VALUES (" . join(", ", map { '?' } @cols) . ")");

    return ( undef, $sth, @cols );
}

sub prepare_mysql_insert {
    my $self = shift;

    my @cols = ( keys %reserved_cols, keys %{ $self->_columns } );

    my $sth = $self->dbh->prepare_cached("INSERT INTO entries (" . join(", ", @cols) . ") VALUES (" . join(", ", map { '?' } @cols) . ") ON DUPLICATE KEY UPDATE " . join(", ", map { "$_ = ?" } grep { $_ ne 'id' } @cols));

    return ( undef, $sth, @cols, grep { $_ ne 'id' } @cols );
}

sub prepare_fallback_insert {
    my $self = shift;

    my @cols = ( keys %reserved_cols, keys %{ $self->_columns } );

    my $ins = $self->dbh->prepare_cached("INSERT INTO entries (" . join(", ", @cols) . ") VALUES (" . join(", ", map { '?' } @cols) . ")");

    my $del = $self->dbh->prepare_cached("DELETE FROM entries WHERE id = ?");

    return ( $del, $ins, @cols );
}

sub update_index {
    my ( $self, $entries ) = @_;

    my $d_sth = $self->dbh->prepare_cached("DELETE FROM gin_index WHERE id = ?");
    my $i_sth = $self->dbh->prepare_cached("INSERT INTO gin_index (id, value) VALUES (?, ?)");

    foreach my $id ( keys %$entries ) {
        $d_sth->execute($id);

        foreach my $value ( @{ $entries->{$id} } ) {
            $i_sth->execute( $id, $value );
        }
    }

    $i_sth->finish;
    $d_sth->finish;
}

sub get {
    my ( $self, @ids ) = @_;

    my $entries = $self->dbh->selectall_hashref("select id, data from entries where id IN (" . join(", ", map { $self->dbh->quote($_) } @ids) . ")", "id");

    return map { $self->deserialize($_->{data}) } @{ $entries }{@ids};
}

sub delete {
    my ( $self, @ids_or_entries ) = @_;

    my @ids = map { ref($_) ? $_->id : $_ } @ids_or_entries;

    if ( $self->extract ) {
        # FIXME rely on cascade delete?
        $self->dbh->do("delete from gin_index where id IN (" . join(", ", map { $self->dbh->quote($_) } @ids) . ")");
    }

    $self->dbh->do("delete from entries where id IN (" . join(", ", map { $self->dbh->quote($_) } @ids) . ")");
}

sub exists {
    my ( $self, @ids ) = @_;

    my $entries = $self->dbh->selectall_hashref("select id from entries where id IN (" . join(", ", map { $self->dbh->quote($_) } @ids) . ")", "id");

    map { exists $entries->{$_} } @ids;
}

sub txn_do {
    my ( $self, $code, %args ) = @_;

    my @ret = eval { shift->storage->txn_do($code) };

    if ( $@ ) {
        if ( my $rb = $args{rollback} ) { $rb->() };
        die $@;
    }

    return @ret;
}

sub txn_begin    { shift->storage->txn_begin(@_) }
sub txn_commit   { shift->storage->txn_commit(@_) }
sub txn_rollback { shift->storage->txn_rollback(@_) }

sub clear {
    my $self = shift;

    $self->dbh->do("delete from entries");
}

sub _select_stream {
    my ( $self, $sql, @bind ) = @_;

    my $sth = $self->dbh->prepare($sql);

    $sth->execute(@bind);

    my $stream = Data::Stream::Bulk::DBI->new( sth => $sth );

    return $stream->filter(sub { [ map { $self->deserialize($_->[0]) } @$_ ] });
}

sub all_entries {
    my $self = shift;
    $self->_select_stream("select data from entries");
}

sub root_entries {
    my $self = shift;
    $self->_select_stream("select data from entries where root");
}

sub child_entries {
    my $self = shift;
    $self->_select_stream("select data from entries where not root");
}

sub simple_search {
    my ( $self, $proto ) = @_;

    my ( $where_clause, @bind ) = $self->sql_abstract->where($proto);

    $self->_select_stream("select data from entries $where_clause", @{ $proto }{ @bind } );
}

sub search {
    my ( $self, $query, @args ) = @_;

    my %args = (
        distinct => $self->distinct,
        @args,
    );

    my %spec = $query->extract_values($self);

    my @v = @{ $spec{values} };

    #if ( $spec{method} eq 'all' ) {
        # make the DB ensure at least one key exists... count(gin_index.id) = @v ?
    #}

    $self->_select_stream("
        select data from entries where id in (
            select id from gin_index where value in (" . join(", ", map { '?' } @v) . ")
        )",
        @v
    );
}

sub fetch_entry { die "TODO" }

sub remove_ids {
    my ( $self, @ids ) = @_;

    die "Deletion the GIN index is handled implicitly by BDB";
}

sub insert_entry {
    my ( $self, $id, @keys ) = @_;

    die "Insertion to the GIN index is handled implicitly by BDB";
}


__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__

=pod

=head1 NAME

KiokuDB::Backend::DBI - L<DBI> backend for L<KiokuDB>

=head1 SYNOPSIS

    my $dir = KiokuDB->connect(
        "dbi:SQLite:dbname=foo",
        columns => [
            # specify extra columns for the 'entries' table
            # in the same format you pass to DBIC's add_columns

            name => {
                data_type => "varchar",
                is_nullable => 1, # probably important
            },
        ],
    );

    $dir->search({ name => "foo" }); # SQL::Abstract

=head1 DESCRIPTION

This backend for L<KiokuDB> leverages existing L<DBI> accessible databases.

The schema is based on two tables, C<entries> and C<gin_index> (the latter is
only used if a L<Search::GIN> extractor is specified).

The C<entries> table has two main columns, C<id> and C<data> (currently in
JSPON format, in the future the format will be pluggable), and additional user
specified columns.

The user specified columns are extracted from inserted objects using a callback
(or just copied for simple scalars), allowing SQL where clauses to be used for
searching.

=head1 COLUMN EXTRACTIONS

The columns are specified using a L<DBIx::Class::ResultSource> instance.

One additional column info parameter is used, C<extract>, which is called as a
method on the inserted object with the column name as the only argument. The
return value from this callback will be used to populate the column.

If the column extractor is omitted then the column will contain a copy of the
entry data key by the same name, if it is a plain scalar. Otherwise the column
will be C<NULL>.

These columns are only used for lookup purposes, only C<data> is consulted when
loading entries.

=head1 SUPPORTED DATABASES

This driver has been tested with MySQL 5 (4.1 should be the minimal supported
version), SQLite 3, and PostgresSQL 8.3.

=head1 ATTRIBUTES

=over 4

=item schema

Created automatically.

This is L<DBIx::Class::Schema> object that is used for schema deployment,
connectivity and transaction handling.

=item connect_info

An array reference whose contents are passed to L<DBIx::Class::Schema/connect>.

If omitted will be created from the attrs C<dsn>, C<user>, C<password> and
C<dbi_attrs>.

=item dsn

=item user

=item password

=item dbi_attrs

Convenience attrs for connecting using L<KiokuDB/connect>.

User in C<connect_info>'s builder.

=item columns

Additional columns, see L</"COLUMN EXTRACTIONS">.

=back

=head1 METHODS

See L<KiokuDB::Backend> and the various roles for more info.

=over 4

=item deploy

Calls L<DBIx::Class::Schema/deploy>.

Deployment to MySQL requires that you specify something like:

    $dir->backend->deploy({ producer_args => { mysql_version => 4 } });

because MySQL versions before 4 did not have support for boolean types, and the
schema emitted by L<SQL::Translator> will not work with the queries used.

=back

=head1 VERSION CONTROL

L<http://github.com/nothingmuch/kiokudb-backend-dbi/>

=head1 AUTHOR

Yuval Kogman E<lt>nothingmuch@woobling.orgE<gt>

=head1 COPYRIGHT

	Copyright (c) 2008 Yuval Kogman. All rights reserved
	This program is free software; you can redistribute
	it and/or modify it under the same terms as Perl itself.

=cut

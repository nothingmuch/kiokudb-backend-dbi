#!/usr/bin/perl

package KiokuDB::Backend::DBI;
use Moose;

use MooseX::Types -declare => [qw(ValidColumnName)];

use MooseX::Types::Moose qw(ArrayRef HashRef Str);

use Moose::Util::TypeConstraints qw(enum);

use Data::Stream::Bulk::DBI;

use KiokuDB::Backend::DBI::Schema;

use SQL::Abstract;

use namespace::clean -except => 'meta';

our $VERSION = "1.10";

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::Delegate
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::TXN
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Query::Simple
    KiokuDB::Backend::Role::Query::GIN
    KiokuDB::Backend::Role::Concurrency::POSIX
    Search::GIN::Extract::Delegate
);
# KiokuDB::Backend::Role::TXN::Nested is not supported by many DBs
# we don't really care though

my @std_cols = qw(id class root tied);
my @reserved_cols = ( @std_cols, 'data' );
my %reserved_cols = ( map { $_ => 1 } @reserved_cols );

subtype ValidColumnName, as Str, where { not exists $reserved_cols{$_} };

sub new_from_dsn {
    my ( $self, $dsn, @args ) = @_;
    $self->new( dsn => "dbi:$dsn", @args );
}

sub BUILD {
    my $self = shift;

    $self->schema; # connect early

    if ( $self->create ) {
        $self->create_tables;
    }
}

has '+serializer' => ( default => "json" ); # to make dumps readable

has create => (
    isa => "Bool",
    is  => "ro",
    default => 0,
);

has 'dsn' => (
    isa => "Str|CodeRef",
    is  => "ro",
);


has [qw(user password)] => (
    isa => "Str",
    is  => "ro",
);

has dbi_attrs => (
    isa => HashRef,
    is  => "ro",
);

has mysql_strict => (
    isa => "Bool",
    is  => "ro",
    default => 1,
);

has sqlite_sync_mode => (
    isa => enum([qw(0 1 2 OFF NORMAL FULL off normal full)]),
    is  => "ro",
    predicate => "has_sqlite_fsync_mode",
);

has on_connect_call => (
    isa => "ArrayRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build_on_connect_call {
    my $self = shift;

    my @call;

    if ( $self->mysql_strict ) {
        push @call, sub {
            my $storage = shift;

            if ( $storage->can("connect_call_set_strict_mode") ) {
                $storage->connect_call_set_strict_mode;
            }
        };
    };

    if ( $self->has_sqlite_fsync_mode ) {
        push @call, sub {
            my $storage = shift;

            if ( $storage->sqlt_type eq 'SQLite' ) {
                $storage->dbh_do(sub { $_[1]->do("PRAGMA synchronous=" . $self->sqlite_sync_mode) });
            }
        };
    }

    return \@call;
}

has dbic_attrs => (
    isa => "HashRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build_dbic_attrs {
    my $self = shift;

    return {
        on_connect_call => $self->on_connect_call,
    };
}

has connect_info => (
    isa => ArrayRef,
    is  => "ro",
    lazy_build => 1,
);

sub _build_connect_info {
    my $self = shift;

    return [ $self->dsn, $self->user, $self->password, $self->dbi_attrs, $self->dbic_attrs ];
}

has schema => (
    isa => "DBIx::Class::Schema",
    is  => "ro",
    lazy_build => 1,
    handles => [qw(deploy)],
);

has schema_hook => (
    isa => "CodeRef|Str",
    is  => "ro",
    predicate => "has_schema_hook",
);

sub _build_schema {
    my $self = shift;

    my $schema = KiokuDB::Backend::DBI::Schema->clone;

    $schema->source("entries")->add_columns(@{ $self->columns });

    if ( $self->has_schema_hook ) {
        my $h = $self->schema_hook;
        $self->$h($schema);
    }

    $schema->connect(@{ $self->connect_info });
}

has storage => (
    isa => "DBIx::Class::Storage::DBI",
    is  => "rw",
    lazy_build => 1,
    handles    => [qw(dbh_do)],
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

has _ordered_columns => (
    isa => "ArrayRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build__ordered_columns {
    my $self = shift;
    return [ @reserved_cols, sort keys %{ $self->_columns } ];
}

has _column_order => (
    isa => "HashRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build__column_order {
    my $self = shift;

    my $cols = $self->_ordered_columns;
    return { map { $cols->[$_] => $_ } 0 .. $#$cols }
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

sub insert {
    my ( $self, @entries ) = @_;

    $self->insert_rows( $self->entries_to_rows(@entries) );

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

    my ( %insert, %update );

    foreach my $t ( \%insert, \%update ) {
        foreach my $col ( @{ $self->_ordered_columns } ) {
            $t->{$col} = [];
        }
    }

    foreach my $entry ( @entries ) {
        my $targ = $entry->prev ? \%update : \%insert;

        my $row = $self->entry_to_row($entry, $targ);
    }

    return \( %insert, %update );
}

sub entry_to_row {
    my ( $self, $entry, $collector ) = @_;

    for (qw(id class tied)) {
        push @{ $collector->{$_} }, $entry->$_;
    }

    push @{ $collector->{root} }, $entry->root ? 1 : 0;

    push @{ $collector->{data} }, $self->serialize($entry);

    my $cols = $self->_columns;

    foreach my $column ( keys %$cols ) {
        my $c = $collector->{$column};
        if ( my $extract = $cols->{$column} ) {
            if ( my $obj = $entry->object ) {
                push @$c, $obj->$extract($column);
                next;
            }
        } elsif ( ref( my $data = $entry->data ) eq 'HASH' ) {
            if ( exists $data->{$column} and not ref( my $value = $data->{$column} ) ) {
                push @$c, $value;
                next;
            }
        }

        push @$c, undef;
    }
}

sub insert_rows {
    my ( $self, $insert, $update ) = @_;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        if ( $self->extract ) {
            if ( my @ids = map { @{ $_->{id} || [] } } $insert, $update ) {
                my $del_gin_sth = $dbh->prepare_cached("DELETE FROM gin_index WHERE id IN (" . join(", ", ('?') x @ids) . ")");

                $del_gin_sth->execute(@ids);

                $del_gin_sth->finish;
            }
        }

        my $bind_attributes = $self->storage->source_bind_attributes($self->schema->source("entries"));

        my %rows = ( insert => $insert, update => $update );

        foreach my $op (qw(insert update)) {
            my $prepare = "prepare_$op";
            my ( $sth, @cols ) = $self->$prepare($dbh);

            my $i = 1;

            foreach my $column_name (@cols) {
                my $attributes = {};

                if( $bind_attributes ) {
                    $attributes = $bind_attributes->{$column_name}
                    if defined $bind_attributes->{$column_name};
                }

                $sth->bind_param_array( $i, $rows{$op}->{$column_name}, $attributes );

                $i++;
            }

            $sth->execute_array({ArrayTupleStatus => []}) or die;

            $sth->finish;
        }
    });
}

sub prepare_insert {
    my ( $self, $dbh ) = @_;

    my @cols = @{ $self->_ordered_columns };

    my $ins = $dbh->prepare("INSERT INTO entries (" . join(", ", @cols) . ") VALUES (" . join(", ", ('?') x @cols) . ")");

    return ( $ins, @cols );
}

sub prepare_update {
    my ( $self, $dbh ) = @_;

    my ( $id, @cols ) = @{ $self->_ordered_columns };

    my $upd = $dbh->prepare("UPDATE entries SET " . join(", ", map { "$_ = ?" } @cols) . " WHERE $id = ?");

    return ( $upd, @cols, $id );
}

sub update_index {
    my ( $self, $entries ) = @_;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        my $i_sth = $dbh->prepare_cached("INSERT INTO gin_index (id, value) VALUES (?, ?)");

        foreach my $id ( keys %$entries ) {
            my $rv = $i_sth->execute_array(
                {ArrayTupleStatus => []},
                $id,
                $entries->{$id},
            );
        }

        $i_sth->finish;
    });
}

sub get {
    my ( $self, @ids ) = @_;

    my %entries;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        my $sth;

        if ( @ids ) {
            $sth = $dbh->prepare_cached("SELECT id, data FROM entries WHERE id IN (" . join(", ", ('?') x @ids) . ")");
            $sth->execute(@ids);
        } else {
            $sth = $dbh->prepare_cached("SELECT id, data FROM entries");
            $sth->execute;
        }

        $sth->bind_columns( \my ( $id, $data ) );

        # not actually necessary but i'm keeping it around for reference:
        #my ( $id, $data );
        #use DBD::Pg qw(PG_BYTEA);
        #$sth->bind_col(1, \$id);
        #$sth->bind_col(2, \$data, { pg_type => PG_BYTEA });

        while ( $sth->fetch ) {
            $entries{$id} = $data;
        }
    });

    return if @ids != keys %entries; # ->rows only works after we're done

    return map { $self->deserialize($_) } @entries{@ids};
}

sub delete {
    my ( $self, @ids_or_entries ) = @_;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        my @ids = map { ref($_) ? $_->id : $_ } @ids_or_entries;

        if ( $self->extract ) {
            # FIXME rely on cascade delete?
            my $sth = $dbh->prepare_cached("DELETE FROM gin_index WHERE id IN (" . join(", ", ('?') x @ids) . ")");
            $sth->execute(@ids);
            $sth->finish;
        }

        my $sth = $dbh->prepare_cached("DELETE FROM entries WHERE id IN (" . join(", ", ('?') x @ids) . ")");
        $sth->execute(@ids);
        $sth->finish;
    });

    return;
}

sub exists {
    my ( $self, @ids ) = @_;

    my %entries;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        my $sth = $dbh->prepare_cached("SELECT id FROM entries WHERE id IN (" . join(", ", ('?') x @ids) . ")");
        $sth->execute(@ids);

        $sth->bind_columns( \( my $id ) );

        $entries{$id} = undef while $sth->fetch;
    });

    map { exists $entries{$_} } @ids;
}

sub txn_begin    { shift->storage->txn_begin(@_) }
sub txn_commit   { shift->storage->txn_commit(@_) }
sub txn_rollback { shift->storage->txn_rollback(@_) }

sub clear {
    my $self = shift;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        $dbh->do("DELETE FROM gin_index");
        $dbh->do("DELETE FROM entries");
    });
}

sub _sth_stream {
    my ( $self, $sql, @bind ) = @_;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;
        my $sth = $dbh->prepare($sql); # can't prepare cached, we don't know when it will be done

        $sth->execute(@bind);

        Data::Stream::Bulk::DBI->new( sth => $sth );
    });
}

sub _select_entry_stream {
    my ( $self, @args ) = @_;

    my $stream = $self->_sth_stream(@args);

    return $stream->filter(sub { [ map { $self->deserialize($_->[0]) } @$_ ] });
}

sub all_entries {
    my $self = shift;
    $self->_select_entry_stream("SELECT data FROM entries");
}

sub root_entries {
    my $self = shift;
    $self->_select_entry_stream("SELECT data FROM entries WHERE root");
}

sub child_entries {
    my $self = shift;
    $self->_select_entry_stream("SELECT data FROM entries WHERE not root");
}

sub _select_id_stream {
    my ( $self, @args ) = @_;

    my $stream = $self->_sth_stream(@args);

    return $stream->filter(sub {[ map { $_->[0] } @$_ ]});
}

sub all_entry_ids {
    my $self = shift;
    $self->_select_id_stream("SELECT id FROM entries");
}

sub root_entry_ids {
    my $self = shift;
    $self->_select_id_stream("SELECT id FROM entries WHERE root");
}

sub child_entry_ids {
    my $self = shift;
    $self->_select_id_stream("SELECT id FROM entries WHERE not root");
}

sub simple_search {
    my ( $self, $proto ) = @_;

    my ( $where_clause, @bind ) = $self->sql_abstract->where($proto);

    $self->_select_entry_stream("SELECT data FROM entries $where_clause", @bind);
}

sub search {
    my ( $self, $query, @args ) = @_;

    my %args = (
        distinct => $self->distinct,
        @args,
    );

    my %spec = $query->extract_values($self);

    my @v = @{ $spec{values} };

    if ( $spec{method} eq 'all' and @v > 1) {
        # for some reason count(id) = ? doesn't work
        return $self->_select_entry_stream("
            SELECT data FROM entries WHERE id IN (
                SELECT id FROM gin_index WHERE value IN (" . join(", ", ('?') x @v) . ") GROUP BY id HAVING COUNT(id) = " . scalar(@v) . "
            )",
            @v
        );
    } else {
        return $self->_select_entry_stream("
            SELECT data FROM entries WHERE id IN (
                SELECT DISTINCT id FROM gin_index WHERE value IN (" . join(", ", ('?') x @v) . ")
            )",
            @v
        );
    }
}

sub fetch_entry { die "TODO" }

sub remove_ids {
    my ( $self, @ids ) = @_;

    die "Deletion the GIN index is handled implicitly";
}

sub insert_entry {
    my ( $self, $id, @keys ) = @_;

    die "Insertion to the GIN index is handled implicitly";
}

sub tables_exist {
    my $self = shift;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        my $filter = ( $self->storage->sqlt_type eq 'SQLite' ? '%' : '' );

        return ( @{ $dbh->table_info($filter, $filter, 'entries', 'TABLE')->fetchall_arrayref } > 0 );
    });
}

sub create_tables {
    my $self = shift;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        unless ( $self->tables_exist ) {
            $self->deploy({ producer_args => { mysql_version => 4.1 } });
        }
    });
}

sub drop_tables {
    my $self = shift;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        $dbh->do("DROP TABLE gin_index");
        $dbh->do("DROP TABLE entries");
    });
}

sub DEMOLISH {
    my $self = shift;

    if ( $self->has_storage ) {
        $self->storage->disconnect;
    }
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__

=pod

=head1 NAME

KiokuDB::Backend::DBI - L<DBI> backend for L<KiokuDB>

=head1 SYNOPSIS

    my $dir = KiokuDB->connect(
        "dbi:mysql:foo",
        user     => "blah",
        password => "moo',
        columns  => [
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

The SQL code is reasonably portable and should work with most databases. Binary
column support is required when using the L<Storable> serializer.

=head2 Transactions

For reasons of performance and ease of use database vendors ship with read
committed transaction isolation by default.

This means that read locks are B<not> acquired when data is fetched from the
database, allowing it to be updated by another writer. If the current
transaction then updates the value it will be silently overwritten.

IMHO this is a much bigger problem when the data is unstructured. This is
because data is loaded and fetched in potentially smaller chunks, increasing
the risk of phantom reads.

Unfortunately enabling truly isolated transaction semantics means that
C<txn_commit> may fail due to a lock contention, forcing you to repeat your
transaction. Arguably this is more correct "read comitted", which can lead to
race conditions.

Enabling repeatable read or serializable transaction isolation prevents
transactions from interfering with eachother, by ensuring all data reads are
performed with a shared lock.

For more information on isolation see
L<http://en.wikipedia.org/wiki/Isolation_(computer_science)>

=head3 SQLite

SQLite provides serializable isolation by default.

L<http://www.sqlite.org/pragma.html#pragma_read_uncommitted>

=head3 MySQL

MySQL provides read committed isolation by default.

Serializable level isolation can be enabled by by default by changing the
C<transaction-isolation> global variable,

L<http://dev.mysql.com/doc/refman/5.1/en/set-transaction.html#isolevel_serializable>

=head3 PostgreSQL

PostgreSQL provides read committed isolation by default.

Repeatable read or serializable isolation can be enabled by setting the default
transaction isolation level, or using the C<SET TRANSACTION> SQL statement.

L<http://www.postgresql.org/docs/8.3/interactive/transaction-iso.html>,
L<http://www.postgresql.org/docs/8.3/interactive/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION>

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

=item serializer

L<KiokuDB::Serializer>. Coerces from a string, too:

    KiokuDB->connect("dbi:...", serializer => "storable");

Defaults to L<KiokuDB::Serializer::JSON>.

=item create

If true the existence of the tables will be checked for and the DB will be
deployed if not.

Defaults to false.

=item extract

An optional L<Search::GIN::Extract> used to create the C<gin_index> entries.

Usually L<Search::GIN::Extract::Callback>.

=item schema_hook

A hook that is called on the backend object as a method with the schema as the
argument just before connecting.

If you need to modify the schema in some way (adding indexes or constraints)
this is where it should be done.

=item sqlite_sync_mode

If this attribute is set and the underlying database is SQLite, then
C<PRAGMA syncrhonous=...> will be issued with this value.

Can be C<OFF>, C<NORMAL> or C<FULL> (SQLite's default), or 0, 1, or 2.

See L<http://www.sqlite.org/pragma.html#pragma_synchronous>.

=item mysql_strict

If true (the default), sets MySQL's strict mode.

This is B<HIGHLY> reccomended, or you may enjoy some of MySQL's more
interesting features, like automatic data loss when the columns are too narrow.

See L<http://dev.mysql.com/doc/refman/5.0/en/server-sql-mode.html> and
L<DBIx::Class::Storage::DBI::mysql> for more details.

=item on_connect_call

See L<DBIx::Class::Storage::DBI>.

This attribute is constructed based on the values of C<mysql_version> and
C<sqlite_sync_mode>, but may be overridden if you need more control.

=item dbic_attrs

See L<DBIx::Class::Storage::DBI>.

Defaults to

    { on_connect_call => $self->on_connect_call }

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

=item drop_tables

Drops the C<entries> and C<gin_index> tables.

=back

=head1 VERSION CONTROL

L<http://github.com/nothingmuch/kiokudb-backend-dbi>

=head1 AUTHOR

Yuval Kogman E<lt>nothingmuch@woobling.orgE<gt>

=head1 COPYRIGHT

    Copyright (c) 2008, 2009 Yuval Kogman, Infinity Interactive. All
    rights reserved This program is free software; you can redistribute
    it and/or modify it under the same terms as Perl itself.

=cut

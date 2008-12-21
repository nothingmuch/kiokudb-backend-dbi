#!/usr/bin/perl

package KiokuDB::Backend::DBI;
use Moose;

use MooseX::Types -declare => [qw(ValidColumnName)];

use MooseX::Types::Moose qw(ArrayRef Str);

use Data::Stream::Bulk::DBI;

use namespace::clean -except => 'meta';

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::JSON
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::TXN
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::UnicodeSafe
    KiokuDB::Backend::Role::Query::Simple
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

has '+utf8' => ( default => 1 );

has storage => (
    isa => "DBIx::Class::Storage::DBI",
    is  => "rw",
    required => 1,
    handles  => [qw(dbh dbh_do)],
);

has columns => (
    isa => ArrayRef[ValidColumnName],
    is  => "ro",
    default => sub { [] },
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

    my @rows = $self->entries_to_rows(@entries);

    $self->insert_rows(@rows);
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
        foreach my $column ( @{ $self->columns } ) {
            # FIXME pluggable handlers
            if ( exists $data->{$column} and not ref( my $value = $data->{$column} ) ) {
                $row{$column} = $value;
            }
        }
    }

    return \%row;
}

sub insert_rows {
    my ( $self, @rows ) = @_;

    my @cols = ( keys %reserved_cols, @{ $self->columns } );

    my $sth = $self->dbh->prepare_cached("INSERT OR REPLACE INTO entries (" . join(", ", @cols) . ") VALUES (" . join(", ", map { '?' } @cols) . ")");
    #my $sth = $self->dbh->prepare_cached("INSERT INTO entries (" . join(", ", @cols) . ") VALUES (" . join(", ", map { '?' } @cols) . ") ON DUPLICATE KEY UPDATE " . join(", ", map { "$_ = ?" } grep { $_ ne 'id' } @cols));

    foreach my $row ( @rows ) {
        $sth->execute( @{ $row }{@cols} );
        #$sth->execute( @{ $row }{@cols}, @{ $row }{grep { $_ ne 'id' } @cols} );
    }

    $sth->finish;
}

sub get {
    my ( $self, @ids ) = @_;

    my $entries = $self->dbh->selectall_hashref("select id, data from entries where id IN (" . join(", ", map { $self->dbh->quote($_) } @ids) . ")", "id");

    return map { $self->deserialize($_->{data}) } @{ $entries }{@ids};
}

sub delete {
    my ( $self, @ids_or_entries ) = @_;

    my @ids = map { ref($_) ? $_->id : $_ } @ids_or_entries;

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

    my $sth = $self->dbh->prepare_cached($sql);

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

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__

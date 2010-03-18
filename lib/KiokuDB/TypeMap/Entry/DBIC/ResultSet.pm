package KiokuDB::TypeMap::Entry::DBIC::ResultSet;
use Moose;

use JSON;
use Scalar::Util qw(weaken);

use namespace::autoclean;

extends qw(KiokuDB::TypeMap::Entry::Naive);

sub compile_collapse_body {
    my ( $self, @args ) = @_;

    my $sub = $self->SUPER::compile_collapse_body(@args);

    return sub {
        my ( $self, %args ) = @_;

        my $rs = $args{object};

        my $clone = $rs->search_rs;

        # clear all cached data
        $clone->set_cache;

        $self->$sub( %args, object => $clone );
    };
}

__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__

=pod

=head1 NAME

KiokuDB::TypeMap::Entry::DBIC::ResultSet - L<KiokuDB::TypeMap::Entry> for
L<DBIx::Class::ResultSet> objects

=head1 DESCRIPTION

The result set is cloned, the clone will have its cache cleared, and then it is
simply serialized normally. This is the only L<DBIx::Class> related object that
is literally stored in the database, as it represents a memory resident object,
not a database resident one.

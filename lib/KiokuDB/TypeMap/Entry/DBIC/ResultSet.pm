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

package KiokuDB::TypeMap::Entry::DBIC::Schema;
use Moose;

use Scalar::Util qw(weaken refaddr);

use namespace::autoclean;

with qw(KiokuDB::TypeMap::Entry);

sub compile {
    my ( $self, $class ) = @_;

    return KiokuDB::TypeMap::Entry::Compiled->new(
        collapse_method => sub {
            my ( $collapser, @args ) = @_;

            $collapser->collapse_first_class(
                sub {
                    my ( $collapser, %args ) = @_;

                    if ( refaddr($collapser->backend->schema) == refaddr($args{object}) ) {
                        return $collapser->make_entry(
                            %args,
                            data => undef,
                        );
                    } else {
                        croak("Referring to foreign DBIC schemas is unsupported");
                    }
                },
                @args,
            );
        },
        expand_method => sub {
            my ( $linker, $entry ) = @_;

            my $schema = $linker->backend->schema;

            $linker->register_object( $entry => $schema );

            return $schema;
        },
        id_method => sub {
            my ( $self, $object ) = @_;

            return 'dbic:schema'; # singleton
        },
        refresh_method => sub { },
        entry => $self,
        class => $class,
    );
}

__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__

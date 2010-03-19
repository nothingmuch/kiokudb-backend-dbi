package KiokuDB::TypeMap::Entry::DBIC::Row;
use Moose;

use JSON;
use Scalar::Util qw(weaken);

use namespace::autoclean;

with qw(KiokuDB::TypeMap::Entry);

has json => (
    isa => "Object",
    is  => "ro",
    default => sub { JSON->new },
);

sub compile {
    my ( $self, $class ) = @_;

    my $json = $self->json;

    return KiokuDB::TypeMap::Entry::Compiled->new(
        collapse_method => sub {
            my ( $collapser, @args ) = @_;

            $collapser->collapse_first_class(
                sub {
                    my ( $collapser, %args ) = @_;

                    my $obj = $args{object};

                    if ( my @objs = values %{ $obj->{_kiokudb_column} } ) {
                        $collapser->visit(@objs);
                    }

                    return $collapser->make_entry(
                        %args,
                        data => $obj,
                    );
                },
                @args,
            );
        },
        expand_method => sub {
            my ( $linker, $entry ) = @_;

            my $obj = $entry->data;

            $linker->register_object( $entry => $obj );

            return $obj;
        },
        id_method => sub {
            my ( $self, $object ) = @_;

            return 'dbic:row:' . $json->encode([ $object->result_source->source_name, $object->id ]);
        },
        refresh_method => sub {
            my ( $linker, $object, $entry, @args ) = @_;
            $object->discard_changes; # FIXME avoid loading '$entry' alltogether
        },
        entry => $self,
        class => $class,
    );
}

__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__

=pod

=head1 NAME

KiokuDB::TypeMap::Entry::DBIC::Row - L<KiokuDB::TypeMap::Entry> for
L<DBIx::Class::Row> objects.

=head1 DESCRIPTION

L<DBIx::Class::Row> objects are resolved symbolically using the special ID
format:

    dbic:row:$json

The C<$json> string is a serialization of:

    [ $result_source_name, @primary_key_values ]

The row objects are not actually written to the KiokuDB storage, as they are
already present in the other tables.

Looking up an object with such an ID is a dynamic lookup that delegates to the
L<DBIx::Class::Schema> and resultsets.

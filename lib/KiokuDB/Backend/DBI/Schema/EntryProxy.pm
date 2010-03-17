package KiokuDB::Backend::DBI::Schema::EntryProxy;

use strict;
use warnings;

use namespace::clean;

use base qw(DBIx::Class);

sub inflate_result {
    my ( $self, $source, $data ) = @_;

    my $handle = $source->schema->kiokudb_handle;

    if ( ref( my $obj = $handle->id_to_object( $data->{id} ) )  ) {
        return $obj;
    } else {
        $handle->lookup($data->{id}); # FIXME we have the data, might as well use it
    }
}

# ex: set sw=4 et:

__PACKAGE__

__END__

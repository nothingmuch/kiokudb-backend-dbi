package DBIx::Class::KiokuDB;

use strict;
use warnings;

use Carp;
use Scalar::Util qw(weaken);

use namespace::clean;

use base qw(DBIx::Class::Core);

sub new {
    my $self = shift->next::method(@_);

    foreach my $key ( $self->result_source->columns ) {
        my $col_info = $self->column_info($key);

        if ( $col_info->{_kiokudb_info} and ref( my $obj = $self->get_column($key) ) ) {
            $self->store_kiokudb_column( $key => $obj );
        }
    }

    return $self;
}

sub insert {
    my ( $self, @args ) = @_;

    my $dir = $self->result_source->schema->kiokudb_handle;
    my $lo = $dir->live_objects;

    if ( my @insert = grep { ref and not $lo->object_to_entry($_) } values %{ $self->{_kiokudb_column} } ) {
        $dir->insert(@insert);
    }

    $self->next::method(@args);
}

sub update {
    my ( $self, @args ) = @_;

    my $dir = $self->result_source->schema->kiokudb_handle;
    my $lo = $dir->live_objects;

    if ( my @insert = grep { ref and not $lo->object_to_entry($_) } values %{ $self->{_kiokudb_column} } ) {
        croak("Can't update object, related KiokuDB objects are not in storage");
    }

    $self->next::method(@args);
}

sub store {
    my ( $self, @args ) = @_;


    if ( my @objects = grep { ref } values %{ $self->{_kiokudb_column} } ) {
        $self->result_source->schema->kiokudb_handle->store(@objects);
    }

    $self->insert_or_update;
}

sub kiokudb_column {
    my ($self, $rel, $cond, $attrs) = @_;

    # assume a foreign key contraint unless defined otherwise
    $attrs->{is_foreign_key_constraint} = 1
        if not exists $attrs->{is_foreign_key_constraint};

    my $fk = defined $cond ? $cond : $rel;

    $self->add_relationship( $rel, 'entries', { 'foreign.id' => "self.$fk" }, $attrs ); # FIXME hardcoded 'entries'

    my $col_info = $self->column_info($fk);

    $col_info->{_kiokudb_info} = {};

    my $accessor = $col_info->{accessor};
    $accessor = $rel unless defined $accessor;

    $self->mk_group_accessors('kiokudb_column' => [ $accessor, $fk]);
}

sub _kiokudb_id_to_object {
    my ( $self, $id ) = @_;

    if ( ref( my $obj = $self->result_source->schema->kiokudb_handle->lookup($id) ) ) {
        return $obj;
    } else {
        croak("No object with ID '$id' found") unless ref $obj;
    }
}

sub _kiokudb_object_to_id {
    my ( $self, $object ) = @_;

    my $dir = $self->result_source->schema->kiokudb_handle;

    if ( my $id = $dir->object_to_id($object) ) {
        return $id;
    } else {
        # generate an ID
        my $collapser = $dir->collapser;
        my $id_method = $collapser->id_method(ref $object);
        my $id = $id = $collapser->$id_method($object);

        # register the ID
        $dir->live_objects->insert( $id => $object );

        return $id;
    }
}

sub get_kiokudb_column {
    my ( $self, $col ) = @_;

    $self->throw_exception("$col is not a KiokuDB column")
        unless exists $self->column_info($col)->{_kiokudb_info};

    return $self->{_kiokudb_column}{$col}
        if defined $self->{_kiokudb_column}{$col};

    if ( defined( my $val = $self->get_column($col) ) ) {
        my $obj = ref $val ? $val : $self->_kiokudb_id_to_object($val);

        # weaken by default, in case there are cycles, the live object scope will
        # take care of this
        weaken( $self->{_kiokudb_column}{$col} = $obj );

        return $obj;
    } else {
        return;
    }
}

sub _set_kiokudb_column {
    my ( $self, $method, $col, $obj ) = @_;

    if ( ref $obj ) {
        $self->$method( $col, $self->_kiokudb_object_to_id($obj) );
        $self->{_kiokudb_column}{$col} = $obj;
    } else {
        $self->$method( $col, undef );
        delete $self->{_kiokudb_column}{$col};
    }

    return $obj;
}

sub set_kiokudb_column {
    my ( $self, @args ) = @_;
    $self->_set_kiokudb_column( set_column => @args );
}

sub store_kiokudb_column {
    my ( $self, @args ) = @_;
    $self->_set_kiokudb_column( store_column => @args );
}

# ex: set sw=4 et:

__PACKAGE__

__END__

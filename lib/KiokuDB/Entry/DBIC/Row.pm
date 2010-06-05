package KiokuDB::Entry::DBIC::Row;
use Moose;

use JSON;

use namespace::autoclean;

extends qw(KiokuDB::Entry::Base);

has '+data' => (
    isa => "DBIx::Class::Row",
    required => 1,
);

has _references => (
    traits => [qw(NoClone Array)],
    isa => "ArrayRef",
    is  => "ro",
    lazy_build => 1,
    handles => {
        references => "elements",
    },
);

sub _build__references {
    my $self = shift;

    return [ map { KiokuDB::Reference->new( id => $_ ) } $self->referenced_ids ];
}


has _referenced_ids => (
    traits => [qw(NoClone Array)],
    isa => "ArrayRef",
    is  => "ro",
    lazy_build => 1,
    handles => {
        referenced_ids => "elements",
    },
);

sub _build__referenced_ids {
    my $self = shift;

    my $row = $self->data;

    my @rels = $row->result_source->relationships;

    my @ids;

    foreach my $rel ( @rels ) {
        my $rs = $row->related_resultset($rel);

        if ( $rs->result_class->isa("DBIx::Class::KiokuDB::EntryProxy") ) {
            push @ids, $rs->get_column("id")->all; # FIXME what about multiple rels?
        } else {
            # for now all DBIC objects are part of the root set for GC purposes
            # but this should be customizable

            #my $source = $rs->result_source;

            #my $name = $source->source_name;
            #my @pk   = $source->primary_columns;

            #push @ids, map {
            #    'dbic:row:' . encode_json([ $name, @{$_}{@pk} ]);
            #} $rs->search({}, {
            #    result_class => 'DBIx::Class::ResultClass::HashRefInflator', # FIXME specialized ID inflator ?
            #    columns      => [ $row->result_source->related_source($rel)->primary_key ],
            #})->all;
        }
    }

    return \@ids;
}


__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__


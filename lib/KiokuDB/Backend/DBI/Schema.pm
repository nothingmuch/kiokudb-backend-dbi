#!/usr/bin/perl

package KiokuDB::Backend::DBI::Schema;
use Moose;

use namespace::clean -except => 'meta';

extends qw(DBIx::Class::Schema);

use DBIx::Class::ResultSource::Table;

my $entries = DBIx::Class::ResultSource::Table->new({ name => "entries" });

$entries->add_columns(
    id    => { data_type => "varchar" },
    data  => { data_type => "blob", is_nullable => 0 },
    class => { data_type => "varchar", is_nullable => 1 },
    root  => { data_type => "boolean", is_nullable => 0 },
    tied  => { data_type => "char", size => 1, is_nullable => 1 },
);

$entries->set_primary_key("id");

my $gin_index = DBIx::Class::ResultSource::Table->new({ name => "gin_index" });

$gin_index->add_columns(
    id => { data_type => "varchar", is_foreign_key => 1 },
    value => { data_type => "varchar" },
);

$gin_index->add_relationship('entry_ids', 'entries',   { 'foreign.id' => 'me.id' });

$entries->sqlt_deploy_callback(sub {
  my ($source, $sqlt_table) = @_;

  $sqlt_table->extra->{mysql_table_type} = "InnoDB";

  if ( $source->schema->storage->sqlt_type eq 'MySQL' ) {
    $sqlt_table->get_field('data')->data_type('longblob');
  }
});

$gin_index->sqlt_deploy_callback(sub {
  my ($source, $sqlt_table) = @_;

  $sqlt_table->extra->{mysql_table_type} = "InnoDB";

  $sqlt_table->add_index( name => 'gin_index_ids', fields => ['id'] )
      or die $sqlt_table->error;

  $sqlt_table->add_index( name => 'gin_index_values', fields => ['value'] )
      or die $sqlt_table->error;
});

__PACKAGE__->register_source( entries => $entries );
__PACKAGE__->register_source( gin_index => $gin_index );

__PACKAGE__

__END__

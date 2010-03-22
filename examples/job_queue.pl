#!/usr/bin/perl

use strict;
use warnings;

=pod

This script demonstrates using L<KiokuDB> to properly serialize a closure,
including maintaining the proper identity of all the referenced objects in the
captured variables.

This feature is used to implement a simple job queue, where the queue
management is handled by DBIC, but the job body is a closure.

Actual job queue features are missing (e.g. marking a job as in progress, etc),
but the point is to show off KiokuDB, not to write a job queue ;-)

=cut

use KiokuDB;

{
    # this is just a mock data
    package MyApp::DB::Result::DataPoint;
    use base qw(DBIx::Class);

    __PACKAGE__->load_components(qw(Core));

    __PACKAGE__->table('data_point');

    __PACKAGE__->add_columns(
        id => { data_type => "integer" },
        value => { data_type => "integer" },
    );

    __PACKAGE__->set_primary_key('id');

    # and a mock result data (the output of a job)
    package MyApp::DB::Result::Output;
    use base qw(DBIx::Class);

    __PACKAGE__->load_components(qw(Core));

    __PACKAGE__->table('output');

    __PACKAGE__->add_columns(
        id => { data_type => "integer" },
        value => { data_type => "integer" },
    );

    __PACKAGE__->set_primary_key('id');

    # this represents a queued or finished job
    package MyApp::DB::Result::Job;
    use base qw(DBIx::Class);

    __PACKAGE__->load_components(qw(Core KiokuDB));
    __PACKAGE__->table('foo');
    __PACKAGE__->add_columns(
        id => { data_type => "integer" },
        description => { data_type => "varchar"  },
        action => { data_type => "varchar" },
        finished => { data_type => "bool", default_value => 0 },
        result => { data_type => "integer", is_nullable => 1, },
    );
    __PACKAGE__->set_primary_key('id');

    __PACKAGE__->kiokudb_column('action');

    __PACKAGE__->belongs_to( result => "MyApp::DB::Result::Output" );

    sub run {
        my $self = shift;

        # run the actual action
        $self->action->($self);

        # mark the job as finished
        $self->finished(1);
        $self->update;
    }

    package MyApp::DB;
    use base qw(DBIx::Class::Schema);

    __PACKAGE__->load_components(qw(Schema::KiokuDB));

    __PACKAGE__->register_class( Job => qw(MyApp::DB::Result::Job));
    __PACKAGE__->register_class( Output => qw(MyApp::DB::Result::Output));
    __PACKAGE__->register_class( DataPoint => qw(MyApp::DB::Result::DataPoint));
}

my $dir = KiokuDB->connect(
    'dbi:SQLite:dbname=:memory:',
    schema => "MyApp::DB",
    create => 1,
);

my $schema = $dir->backend->schema;

# create some data
$schema->txn_do(sub {
    my $rs = $schema->resultset("DataPoint");

    $rs->create({ value => 4 });
    $rs->create({ value => 3 });
    $rs->create({ value => 2 });
    $rs->create({ value => 50 });
});

# queue a job
$dir->txn_do( scope => 1, body => sub {
    my $small_numbers = $schema->resultset("DataPoint")->search({ value => { "<=", 10 } });

    # create a closure for the job:
    my $action = sub {
        my $self = shift;

        my $sum = 0;

        # small_numbers is a closure variable, which will be saved implicitly
        # as a KiokuDB object
        while ( my $data_point = $small_numbers->next ) {
            $sum += $data_point->value;
        }

        # $schema is also restored properly
        $self->result( $schema->resultset("Output")->create({ value => $sum }) );
    };

    # we can simply store the closure in the DB
    $schema->resultset("Job")->create({
        description => "sum some small numbers",
        action      => $action,
    });
});

# run a job
# this can be done in worker process, obviously (just change :memory: to a real
# file)
$dir->txn_do( scope => 1, body => sub {
    my $jobs = $schema->resultset("Job")->search({ finished => 0 });

    my $job = $jobs->search(undef, { limit => 1 })->single;

    $job->run();

    my $result = $job->result;

    # ...
});

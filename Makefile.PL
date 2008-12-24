#!/usr/bin/perl -w

use strict;

use ExtUtils::MakeMaker;

WriteMakefile(
    NAME         => 'KiokuDB::Backend::DBI',
    VERSION_FROM => 'lib/KiokuDB/Backend/DBI.pm',
    INSTALLDIRS  => 'site',
    SIGN         => 1,
    PL_FILES     => { },
    PREREQ_PM    => {
        'Moose' => 0,
        'MooseX::Types' => '0.08',
        'MooseX::Types::Moose' => 0,

        'KiokuDB' => '0.13',
        'DBIx::Class' => '0.08010', # FIXME bump when changing to use sqlt_deploy_callback
        'DBI' => '1.607', # execute_array etc

        'Data::Stream::Bulk' => 0,

        'Test::use::ok' => 0,
        'Test::More' => 0,
        'Test::TempDir' => 0,

        'SQL::Abstract' => 0,
        'Search::GIN' => 0,
    },
);


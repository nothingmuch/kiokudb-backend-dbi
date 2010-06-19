package inc::DBICOptionalDeps;
use Moose;

extends 'Dist::Zilla::Plugin::MakeMaker::Awesome';

override _build_MakeFile_PL_template => sub {
    my ($self) = @_;

    my $template = super();

    my $injected = <<'INJECT';
require DBIx::Class::Optional::Dependencies;

$WriteMakefileArgs{PREREQ_PM} = {
    %{ $WriteMakefileArgs{PREREQ_PM} || {} },
    %{ DBIx::Class::Optional::Dependencies->req_list_for ('deploy') },
};

INJECT

    $template =~ s{(?=WriteMakefile\s*\()}{$injected};

    return $template;
};

__PACKAGE__->meta->make_immutable;


package Yote::ObjectStore::HistoryLogger;

use strict;
use warnings;

use Data::Dumper;

sub new {
    my ($pkg, $file) = @_;
    open my $append, '>>', $file or die "Error opening '$file': $@ $!";
    return bless { append => $append }, $pkg;
}

sub log {
    my ($self, $txt) = @_;
    my $out = $self->{append};
    print $out "$txt\n";
}


1;

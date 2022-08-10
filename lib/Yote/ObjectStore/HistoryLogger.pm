package Yote::ObjectStore::HistoryLogger;

use strict;
use warnings;

use Data::Dumper;

sub new {
    my ($pkg, $dir) = @_;
    my $files = {};
    for my $logtype (qw( history error warning message)) {
        my $file = "$dir/$logtype.log";
        open my $append, '>>', $file or die "Error opening '$file': $@ $!";
        $files->{$logtype} = $append;
    }
    return bless $files, $pkg;
}

sub history {
    my ($self, $action, $id, $data) = @_;
    my $out = $self->{history};
    print $out join( " ", $action, $id, $data )."\n";
}


1;

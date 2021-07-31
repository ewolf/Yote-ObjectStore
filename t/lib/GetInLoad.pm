package GetInLoad;

use base 'Yote::ObjectStore::Container';

sub _load {
    my $self = shift;
    $self->get_fred([]);
}

1;

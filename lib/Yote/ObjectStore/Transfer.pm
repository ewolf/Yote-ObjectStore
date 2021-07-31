package Yote::ObjectStore::Transfer;

#
# this is a tool for scripting.
#
# it can report object class statistics
# from all the children of a given object.
#
# it can copy an object from one objectstore
# to an other. that object must still be joined
# into the other objectstore's tree to be
# reachable.
#


use strict;
use warnings;

no warnings 'recursion';

sub new {
    my $pkg = shift;
    return bless {
        sourceid2dest   => {},
        sourceid2destid => {},
        seen            => {},
    }, $pkg;
} #new

sub set_class_map {
    my ($self, %map) = @_;
    $self->{class_map} = {%map};
} #set_class_map

sub translate_class {
    my ($self, $cls) = @_;
    if (exists $self->{class_map}{$cls}) {
        return $self->{class_map}{$cls};
    }
    return $cls;
}

# methods are : id, fields
sub set_source_adapter {
    my ($self, %args) = @_;
    $self->{source_adapter} = {%args};
} #set_source_adapter

# methods are : inject, create_container, id
sub set_dest_adapter {
    my ($self, %args) = @_;
    $self->{dest_adapter} = {%args};
} #set_dest_adapter

sub reset {
    my $self = shift;
    $self->{sourceid2dest} = {};
    $self->{sourceid2destid} = {};
    $self->{seen}        = {};
} #reset

sub gather_stats {
    my ($self, $source_obj, $stats) = @_;

    $stats //= {};

    my $r = ref $source_obj;

    return $stats unless $r;

    my $id = $self->{source_adapter}{id}->($source_obj);
    return $stats if $self->{seen}{$id}++;
    
    $stats->{$r}++;

    if ($r eq 'ARRAY') {
        for my $item (@$source_obj) {
            $self->gather_stats( $item, $stats );
        }
    }
    elsif ($r eq 'HASH') {
        for my $key (keys %$source_obj) {
            $self->gather_stats( $source_obj->{$key}, $stats );
        }
    }
    else {
        my $fields = $self->{source_adapter}{fields}->($source_obj);
        for my $field (@$fields) {
            $self->gather_stats( $source_obj->get( $field ), $stats );
        }
    }
    return $stats;
} #gather_stats

sub sourceid2destid {
    my ($self,$id) = @_;
    return $self->{sourceid2destid}{$id};
}

# returns destination object
sub transfer {
    my ($self, $source_obj) = @_;

    my $r = ref $source_obj;
    return $source_obj unless $r;

    my $id = $self->{source_adapter}{id}->($source_obj);
    print STDERR "transfer $id ($r)\n";

    return $self->{sourceid2dest}{$id} if $self->{sourceid2dest}{$id};

    my $dest_obj;
    if ($r eq 'ARRAY') {
        $dest_obj = $self->{dest_adapter}{injest}->([]);
        $self->{sourceid2dest}{$id} = $dest_obj;
        $self->{sourceid2destid}{$id} = $dest_obj;
        for my $source_item (@$source_obj) {
            push @$dest_obj, $self->transfer( $source_item );
        }
    }
    elsif ($r eq 'HASH') {
        $dest_obj = $self->{dest_adapter}{injest}->({});
        $self->{sourceid2dest}{$id} = $dest_obj;
        for my $key (keys %$source_obj) {
            $dest_obj->{$key} = $self->transfer( $source_obj->{$key} );
        }
    }
    else {
        $dest_obj = $self->{dest_adapter}{create_container}->($self->translate_class($r));
        $self->{sourceid2dest}{$id} = $dest_obj;
        my $fields = $self->{source_adapter}{fields}->($source_obj);
        for my $field (@$fields) {
            $dest_obj->set( $field, $self->transfer( $source_obj->get( $field ) ) );
        }
    }
    return $dest_obj;
} #transfer

1;

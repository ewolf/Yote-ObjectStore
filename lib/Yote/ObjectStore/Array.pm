package Yote::ObjectStore::Array;

use strict;
use warnings;
no warnings 'uninitialized';

use Tie::Array;

use constant {
    ID          => 0,
    DATA        => 1,
    OBJ_STORE   => 2,
};

sub _dirty {
    my $self = shift;
    $self->[OBJ_STORE]->dirty( $self->[ID], $self );
}

sub __data {
    return shift->[DATA];
}

sub id {
    return shift->[ID];
}

sub TIEARRAY {
    my( $pkg, $id, $store, @list ) = @_;
    
    return bless [
        $id,
        [@list],
	$store,
	undef,
	], $pkg;

} #TIEARRAY

sub FETCH {
    my( $self, $idx ) = @_;

    my $data = $self->[DATA];
    return undef if $idx >= @$data;

    return $self->[OBJ_STORE]->xform_out( $self->[DATA][$idx] );
    
} #FETCH

sub FETCHSIZE {
    return scalar( @{shift->[DATA]} );
}

sub STORE {
    my( $self, $idx, $val ) = @_;
    my $inval = $self->[OBJ_STORE]->xform_in( $val );
    if ($inval ne $self->[DATA][$idx]) {
        $self->_dirty;
    }
    $self->[DATA][$idx] = $inval;
    return $val;
} #STORE

sub STORESIZE {
    my( $self, $size ) = @_;

    my $data = $self->[DATA];
    $#$data = $size - 1;

} #STORESIZE

sub EXISTS {
    my( $self, $idx ) = @_;
    return exists $self->[DATA][$idx];
} #EXISTS

sub DELETE {
    my( $self, $idx ) = @_;
    (exists $self->[DATA]->[$idx]) && $self->_dirty;
    my $val = delete $self->[DATA][$idx];
    return $self->[OBJ_STORE]->xform_out( $val );
} #DELETE

sub CLEAR {
    my $self = shift;
    my $data = $self->[DATA];
    @$data && $self->_dirty;
    @$data = ();
}
sub PUSH {
    my( $self, @vals ) = @_;
    my $data = $self->[DATA];
    if (@vals) {
        $self->_dirty;
    }
    my $ret =  push @$data,
        map { $self->[OBJ_STORE]->xform_in($_) } @vals;
    return $ret;
}
sub POP {
    my $self = shift;
    my $item = pop @{$self->[DATA]};
    $self->_dirty;
    return $self->[OBJ_STORE]->xform_out( $item );
}
sub SHIFT {
    my( $self ) = @_;
    my $item = shift @{$self->[DATA]};
    $self->_dirty;
    return $self->[OBJ_STORE]->xform_out( $item );
}

sub UNSHIFT {
    my( $self, @vals ) = @_;
    my $data = $self->[DATA];
    @vals && $self->_dirty;
    return unshift @$data,
	map { $self->[OBJ_STORE]->xform_in($_) } @vals;
}

sub SPLICE {
    my( $self, $offset, $remove_length, @vals ) = @_;
    my $data = $self->[DATA];
    $self->_dirty;
    return map { $self->[OBJ_STORE]->xform_out($_) } splice @$data, $offset, $remove_length,
	map { $self->[OBJ_STORE]->xform_in($_) } @vals;
} #SPLICE

sub EXTEND {}

sub __freezedry {
    # packs into
    #   I - length of classname
    #   (a<length of classname>)
    #   I - number of segments
    #   I x number of segments - each segment length
    #   (a<segment length>) x number of segments
    my $self = shift;

    my $r = ref( $self );
    my $cls_length = do { use bytes; length($r); };
    
    my (@data) = (map { defined($_) ? $_ : 'u' } @{$self->__data});

    my (@lengths) = map { do { use bytes; length($_) } } @data;

    my $pack_template = "I(a$cls_length)I". (1+scalar(@data)) . join( "", map { "(a$_)" } @lengths );

    return pack $pack_template, $cls_length, $r, scalar(@lengths), @lengths, @data;
}

sub __reconstitute {
    my ($self, $id, $data, $store ) = @_;

    my $class_length = unpack "I", $data;
    (undef, my $class) = unpack "I(a$class_length)", $data;

    (undef, undef, my $part_count) = unpack "I(a$class_length)I", $data;
    my $unpack_template = "I(a$class_length)I".(1+scalar($part_count));
    (undef, undef, undef, my @sizes) = unpack $unpack_template, $data;

    $unpack_template .= join( "", map { "(a$_)" } @sizes );

    my( @parts ) = unpack $unpack_template, $data;

    splice @parts, 0, ($part_count+3);

    my @array;
    tie @array, 'Yote::ObjectStore::Array', $id, $store, @parts;
    return \@array;
}

"ARRAY ARRAY ARRAY";

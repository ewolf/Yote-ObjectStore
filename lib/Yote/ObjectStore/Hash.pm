package Yote::ObjectStore::Hash;

use strict;
use warnings;
no warnings 'uninitialized';

use Tie::Hash;

use constant {
    ID         => 0,
    DATA       => 1,
    OBJ_STORE  => 2,
    NEXT       => 3,
};

sub _dirty {
    my $self = shift;
    $self->[OBJ_STORE]->dirty( $self->[ID], $self );
} #_dirty

sub __data {
    return shift->[DATA];
}

sub id {
    return shift->[ID];
}

sub TIEHASH {
    my( $pkg, $id, $store, %hash ) = @_;
    return bless [ $id,
		   { %hash },
		   $store,
		   undef,
	], $pkg;
} #TIEHASH

sub CLEAR {
    my $self = shift;
    my $data = $self->[DATA];
    $self->_dirty if scalar( keys %$data );
    %$data = ();
} #CLEAR

sub DELETE {
    my( $self, $key ) = @_;

    my $data = $self->[DATA];
    return undef unless exists $data->{$key};
    $self->_dirty;
    my $oldval = delete $data->{$key};
    return $self->[OBJ_STORE]->xform_out( $oldval );
} #DELETE


sub EXISTS {
    my( $self, $key ) = @_;
    return exists $self->[DATA]{$key};
} #EXISTS

sub FETCH {
    my( $self, $key ) = @_;
    return $self->[OBJ_STORE]->xform_out( $self->[DATA]{$key} );
} #FETCH

sub STORE {
    my( $self, $key, $val ) = @_;
    my $data = $self->[DATA];
    my $oldval = $data->{$key};
    my $inval = $self->[OBJ_STORE]->xform_in( $val );
    ( $inval ne $oldval ) && $self->_dirty;
    $data->{$key} = $inval;
    return $val;
} #STORE

sub FIRSTKEY {
    my $self = shift;

    my $data = $self->[DATA];
    my $a = scalar keys %$data; #reset the each
    my( $k, $val ) = each %$data;
    return $k;
} #FIRSTKEY

sub NEXTKEY  {
    my $self = shift;
    my $data = $self->[DATA];
    my( $k, $val ) = each %$data;
    return $k;
} #NEXTKEY

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
    
    my (@data) = %{$self->__data};

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

    my %hash;
    tie %hash, 'Yote::ObjectStore::Hash', $id, $store, @parts;
    return \%hash;
}

"HASH IT OUT";
